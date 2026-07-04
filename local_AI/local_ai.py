import os
import json
import boto3
import requests
from botocore.config import Config
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

#rag_prompt 수정 필수
SLACK_BOT_TOKEN = "" # 슬랙 봇 토큰
SLACK_APP_TOKEN = "" # 슬랙 앱 토큰


# [인프라 관로 확장] 동시 요청 처리를 위해 네트워크 풀 크기를 20으로 확장
aws_pipeline_config = Config(max_pool_connections=20)

s3 = boto3.client('s3', config=aws_pipeline_config)
bedrock = boto3.client('bedrock-runtime', region_name='ap-northeast-2', config=aws_pipeline_config)
s3vectors = boto3.client('s3vectors', region_name='ap-northeast-2', config=aws_pipeline_config)import os
import json
import boto3
import requests
from botocore.config import Config
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler


SLACK_BOT_TOKEN = "" # 슬랙 봇 토큰
SLACK_APP_TOKEN = "" # 슬랙 앱 토큰


# [인프라 관로 확장] 동시 요청 처리를 위해 네트워크 풀 크기를 20으로 확장
aws_pipeline_config = Config(max_pool_connections=20)

s3 = boto3.client('s3', config=aws_pipeline_config)
bedrock = boto3.client('bedrock-runtime', region_name='ap-northeast-2', config=aws_pipeline_config)
s3vectors = boto3.client('s3vectors', region_name='ap-northeast-2', config=aws_pipeline_config)

BUCKET_STATE = "" # 범용 버킷이름
BUCKET_VECTOR = "" # 벡터 버킷이름
INDEX_NAME = "" # 벡터 버킷 인덱스 이름

OLLAMA_URL = "" # OLLAMA 호출 URL
MODEL_NAME = "" # 모델 이름 Ex) gemma4:e2b

# [Slack App Initializer] 소켓 모드로 구동되는 슬랙 앱 인스턴스 실행
slack_app = App(token=SLACK_BOT_TOKEN)


def get_query_embedding(question):
    bedrock_payload = {"inputText": question, "dimensions": 1024}
    response = bedrock.invoke_model(
        body=json.dumps(bedrock_payload),
        modelId="amazon.titan-embed-text-v2:0",
        accept="application/json",
        contentType="application/json"
    )
    return json.loads(response.get("body").read()).get("embedding")


def retrieve_relevant_context(question_vector, top_k=7):
    try:
        # returnMetadata와 returnDistance를 모두 켜서 텍스트와 점수를 확정 인출합니다.
        response = s3vectors.query_vectors(
            vectorBucketName=BUCKET_VECTOR,
            indexName=INDEX_NAME,
            queryVector={'float32': question_vector},  
            topK=top_k,
            returnMetadata=True,
            returnDistance=True  # [핵심] 이 가드레일을 켜야 AWS가 거리 점수를 반환합니다!
        )
        
        results = []
        for h in response.get('vectors', []):
            distance = h.get('distance', 1.0)
            # 코사인 거리(0=똑같음, 1=남남)를 인간이 보기 편한 유사도 점수(1=똑같음, 0=남남)로 변환
            similarity_score = 1.0 - distance
            
            results.append({
                "score": similarity_score,
                "text": h.get('metadata', {}).get('text_chunk', ''), 
                "source": h.get('metadata', {}).get('file_name', 'Unknown')
            })
        return results
        
    except Exception as e:
        print(f"[SEARCH ERROR] S3 Vectors 검색 실패: {str(e)}")
        return []


def core_rag_pipeline(user_question):
    """기존의 하이브리드 RAG 핵심 연산 코어 라우터"""
    is_metadata_query = any(keyword in user_question for keyword in ["최근", "최신", "목록", "리스트", "마지막", "올라온", "파일들"])
	
    # 메타 데이터 관련 질문 시 S3 의 카탈로그 파일 조회
    if is_metadata_query:
        try:
            catalog_obj = s3.get_object(Bucket=BUCKET_STATE, Key="master_file_catalog.json")
            catalog_data = json.loads(catalog_obj['Body'].read().decode('utf-8'))
            files = catalog_data.get("files", [])
            if files:
                context_str = "현재 유저의 지식 창고에 가장 최근 업데이트 완료된 마스터 파일 대장 목록입니다:\n"
                for i, f in enumerate(files[:5]):
                    context_str += f"{i+1}. 파일명: {f['path']} (수정시각: {f['time']})\n"
            else:
                context_str = "현재 장부 대장에 등록된 파일 기록이 없습니다.\n"
        except Exception:
            context_str = "S3 장부 카탈로그를 조회할 수 없습니다.\n"
    else:
        question_vector = get_query_embedding(user_question)
        relevant_contexts = retrieve_relevant_context(question_vector, top_k=3)
        context_str = ""
        for i, ctx in enumerate(relevant_contexts):
            context_str += f"[출처: {ctx['source']} | 유사도: {ctx['score']:.4f}]\n- {ctx['text']}\n\n"
    print(f"\n[DATA DEBUG] S3 Vectors에서 인출해온 실시간 문맥 조각:\n{context_str}====================================")
	
	# 프롬프트
    rag_prompt = f"""입력할 내용.

[제공된 내부 지식 문맥]:
{context_str}

[질문]:
{user_question}

[답변]:"""

    try:
        ollama_response = requests.post(OLLAMA_URL, json={"model": MODEL_NAME, "prompt": rag_prompt, "stream": False}, timeout=300)
        return ollama_response.json().get("response")
    except Exception as e:
        return f"로컬 Ollama 엔진 통신 실패: {str(e)}"


# ======================================================================
# 📡 [Slack Event Controller] 슬랙 채널에서 @봇이름 으로 맨션하면 이 핸들러가 가동됩니다.
# ======================================================================
@slack_app.event("app_mention")
def handle_mention_events(event, say):
    raw_text = event.get("text", "")
    
    # 슬랙 맨션 태그(<@UXXXXXX>)를 잘라내고 유저의 진짜 질문 텍스트만 정제 추출
    user_question = raw_text.split(">")[-1].strip()
    
    if not user_question:
        say(text="질문 내용이 확인되지 않습니다. 봇을 멘션한 뒤 질문을 적어주세요!", thread_ts=event.get("ts"))
        return
        
    print(f"\n[Slack Ingress] 슬랙 채널로부터 질문 접수 완료: '{user_question}'")
    
    # ⏳ 슬랙 사용자 인터페이스 경험을 위해 "생각 중..." 임시 메시지 먼저 투척
    initial_receipt = say(
        text="로컬 RAG 파이프라인 가동 중... 잠시만 기다려주세요 (Ollama 추론 중)", 
        thread_ts=event.get("ts")  # 이 옵션이 실시간 스레드 결속을 만들어줍니다!
    )
    
    # 고성능 RAG 코어 엔진 격발 및 추론 결과 도출
    ai_answer = core_rag_pipeline(user_question)
    
    # 슬랙방에 최종 인프라 지식 답변 배출 완공
    say(text=ai_answer, thread_ts=event.get("ts"))  # 스레드 댓글 형태로 깔끔하게 답변 전송
    print("[Slack Egress] 슬랙 채널로 로컬 LLM 추론 답변 송신 완공!")


# ======================================================================
# 🏁 로컬 실시간 웹소켓 리스너 상시 구동 가동선
# ======================================================================
if __name__ == "__main__":
    print("[Socket Mode Engine] 노트북 내부 하이브리드 RAG 슬랙 봇 가동 시작...")
    print("외부 tunnel(ngrok) 없이 슬랙 공식 웹소켓 관로와 안전하게 연결되었습니다.")
    handler = SocketModeHandler(slack_app, SLACK_APP_TOKEN)
    handler.start()

BUCKET_STATE = "" # 범용 버킷이름
BUCKET_VECTOR = "" # 벡터 버킷이름
INDEX_NAME = "" # 벡터 버킷 인덱스 이름

OLLAMA_URL = "" # OLLAMA 호출 URL
MODEL_NAME = "" # 모델 이름 Ex) gemma4:e2b

# [Slack App Initializer] 소켓 모드로 구동되는 슬랙 앱 인스턴스 실행
slack_app = App(token=SLACK_BOT_TOKEN)


def get_query_embedding(question):
    bedrock_payload = {"inputText": question, "dimensions": 1024}
    response = bedrock.invoke_model(
        body=json.dumps(bedrock_payload),
        modelId="amazon.titan-embed-text-v2:0",
        accept="application/json",
        contentType="application/json"
    )
    return json.loads(response.get("body").read()).get("embedding")


def retrieve_relevant_context(question_vector, top_k=7):
    try:
        # returnMetadata와 returnDistance를 모두 켜서 텍스트와 점수를 확정 인출합니다.
        response = s3vectors.query_vectors(
            vectorBucketName=BUCKET_VECTOR,
            indexName=INDEX_NAME,
            queryVector={'float32': question_vector},  
            topK=top_k,
            returnMetadata=True,
            returnDistance=True  # [핵심] 이 가드레일을 켜야 AWS가 거리 점수를 반환합니다!
        )
        
        results = []
        for h in response.get('vectors', []):
            distance = h.get('distance', 1.0)
            # 코사인 거리(0=똑같음, 1=남남)를 인간이 보기 편한 유사도 점수(1=똑같음, 0=남남)로 변환
            similarity_score = 1.0 - distance
            
            results.append({
                "score": similarity_score,
                "text": h.get('metadata', {}).get('text_chunk', ''), 
                "source": h.get('metadata', {}).get('file_name', 'Unknown')
            })
        return results
        
    except Exception as e:
        print(f"[SEARCH ERROR] S3 Vectors 검색 실패: {str(e)}")
        return []


def core_rag_pipeline(user_question):
    """기존의 하이브리드 RAG 핵심 연산 코어 라우터"""
    is_metadata_query = any(keyword in user_question for keyword in ["최근", "최신", "목록", "리스트", "마지막", "올라온", "파일들"])
	
    # 메타 데이터 관련 질문 시 S3 의 카탈로그 파일 조회
    if is_metadata_query:
        try:
            catalog_obj = s3.get_object(Bucket=BUCKET_STATE, Key="master_file_catalog.json")
            catalog_data = json.loads(catalog_obj['Body'].read().decode('utf-8'))
            files = catalog_data.get("files", [])
            if files:
                context_str = "현재 유저의 지식 창고에 가장 최근 업데이트 완료된 마스터 파일 대장 목록입니다:\n"
                for i, f in enumerate(files[:5]):
                    context_str += f"{i+1}. 파일명: {f['path']} (수정시각: {f['time']})\n"
            else:
                context_str = "현재 장부 대장에 등록된 파일 기록이 없습니다.\n"
        except Exception:
            context_str = "S3 장부 카탈로그를 조회할 수 없습니다.\n"
    else:
        question_vector = get_query_embedding(user_question)
        relevant_contexts = retrieve_relevant_context(question_vector, top_k=3)
        context_str = ""
        for i, ctx in enumerate(relevant_contexts):
            context_str += f"[출처: {ctx['source']} | 유사도: {ctx['score']:.4f}]\n- {ctx['text']}\n\n"
    print(f"\n[DATA DEBUG] S3 Vectors에서 인출해온 실시간 문맥 조각:\n{context_str}====================================")
	
	# 프롬프트
    rag_prompt = f"""프롬프트에 넣을 내용입니다

[제공된 내부 지식 문맥]:
{context_str}

[질문]:
{user_question}

[답변]:"""

    try:
        ollama_response = requests.post(OLLAMA_URL, json={"model": MODEL_NAME, "prompt": rag_prompt, "stream": False}, timeout=300)
        return ollama_response.json().get("response")
    except Exception as e:
        return f"로컬 Ollama 엔진 통신 실패: {str(e)}"


# ======================================================================
# 📡 [Slack Event Controller] 슬랙 채널에서 @봇이름 으로 맨션하면 이 핸들러가 가동됩니다.
# ======================================================================
@slack_app.event("app_mention")
def handle_mention_events(event, say):
    raw_text = event.get("text", "")
    
    # 슬랙 맨션 태그(<@UXXXXXX>)를 잘라내고 유저의 진짜 질문 텍스트만 정제 추출
    user_question = raw_text.split(">")[-1].strip()
    
    if not user_question:
        say(text="질문 내용이 확인되지 않습니다. 봇을 멘션한 뒤 질문을 적어주세요!", thread_ts=event.get("ts"))
        return
        
    print(f"\n[Slack Ingress] 슬랙 채널로부터 질문 접수 완료: '{user_question}'")
    
    # ⏳ 슬랙 사용자 인터페이스 경험을 위해 "생각 중..." 임시 메시지 먼저 투척
    initial_receipt = say(
        text="로컬 RAG 파이프라인 가동 중... 잠시만 기다려주세요 (Ollama 추론 중)", 
        thread_ts=event.get("ts")  # 이 옵션이 실시간 스레드 결속을 만들어줍니다!
    )
    
    # 고성능 RAG 코어 엔진 격발 및 추론 결과 도출
    ai_answer = core_rag_pipeline(user_question)
    
    # 슬랙방에 최종 인프라 지식 답변 배출 완공
    say(text=ai_answer, thread_ts=event.get("ts"))  # 스레드 댓글 형태로 깔끔하게 답변 전송
    print("[Slack Egress] 슬랙 채널로 로컬 LLM 추론 답변 송신 완공!")


# ======================================================================
# 🏁 로컬 실시간 웹소켓 리스너 상시 구동 가동선
# ======================================================================
if __name__ == "__main__":
    print("[Socket Mode Engine] 노트북 내부 하이브리드 RAG 슬랙 봇 가동 시작...")
    print("외부 tunnel(ngrok) 없이 슬랙 공식 웹소켓 관로와 안전하게 연결되었습니다.")
    handler = SocketModeHandler(slack_app, SLACK_APP_TOKEN)
    handler.start()