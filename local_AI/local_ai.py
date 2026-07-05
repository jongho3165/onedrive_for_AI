import os
import json
import io
import boto3
import requests
from botocore.config import Config
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from dotenv import load_dotenv

# 환경 변수 로드
load_dotenv()

# 자격 증명 관리 설정
SLACK_BOT_TOKEN = os.environ.get("SLACK_BOT_TOKEN", "")
SLACK_APP_TOKEN = os.environ.get("SLACK_APP_TOKEN", "")

AWS_BUCKET_STATE = os.environ.get("AWS_BUCKET_STATE", "")
AWS_BUCKET_VECTOR = os.environ.get("AWS_BUCKET_VECTOR", "")
INDEX_NAME = os.environ.get("AWS_INDEX_NAME", "")

OLLAMA_URL = os.environ.get("OLLAMA_URL", "")
OLLAMA_MODEL_NAME = os.environ.get("OLLAMA_MODEL_NAME", "")

MS_CLIENT_ID = os.environ.get("MS_CLIENT_ID", "")
MS_CLIENT_SECRET = os.environ.get("MS_CLIENT_SECRET", "")
TARGET_FOLDER = os.environ.get("MS_TARGET_FOLDER", "Reports").strip()
TOKEN_URL = "https://login.microsoftonline.com/common/oauth2/v2.0/token"

# 인프라 커넥션 풀 확장
aws_pipeline_config = Config(max_pool_connections=20)

s3 = boto3.client('s3', config=aws_pipeline_config)
bedrock = boto3.client('bedrock-runtime', region_name='ap-northeast-2', config=aws_pipeline_config)
s3vectors = boto3.client('s3vectors', region_name='ap-northeast-2', config=aws_pipeline_config)

slack_app = App(token=SLACK_BOT_TOKEN)


def get_fresh_access_token():
    try:
        token_obj = s3.get_object(Bucket=AWS_BUCKET_STATE, Key="ms_graph_refresh_token.txt")
        old_refresh_token = token_obj['Body'].read().decode('utf-8').strip()

        payload = {
            "client_id": MS_CLIENT_ID,
            "client_secret": MS_CLIENT_SECRET,
            "grant_type": "refresh_token",
            "refresh_token": old_refresh_token,
            "scope": "files.readwrite offline_access"
        }

        token_response = requests.post(TOKEN_URL, data=payload).json()
        new_refresh_token = token_response.get("refresh_token")

        if new_refresh_token:
            s3.put_object(Bucket=AWS_BUCKET_STATE, Key="ms_graph_refresh_token.txt", Body=new_refresh_token.encode('utf-8'))

        return token_response.get("access_token")
    except Exception as token_err:
        print(f"[TOKEN ERROR] MS Graph 엑세스 토큰 갱신 실패: {str(token_err)}")
        return None


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
        response = s3vectors.query_vectors(
            vectorBucketName=AWS_BUCKET_VECTOR,
            indexName=INDEX_NAME,
            queryVector={'float32': question_vector},  
            topK=top_k,
            returnMetadata=True,
            returnDistance=True  
        )
        results = []
        for h in response.get('vectors', []):
            distance = h.get('distance', 1.0)
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
    is_metadata_query = any(keyword in user_question for keyword in ["최근", "최신", "목록", "리스트", "마지막", "올라온", "파일들"])
    knowledge_found = True
    
    if is_metadata_query:
        try:
            catalog_obj = s3.get_object(Bucket=AWS_BUCKET_STATE, Key="master_file_catalog.json")
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
            
        if not relevant_contexts or not context_str.strip():
            knowledge_found = False
            
    print(f"\n[DATA DEBUG] S3 Vectors에서 인출해온 실시간 문맥 조각:\n{context_str}====================================")
    
    rag_prompt = f"""너는 장종호 님의 개인 원드라이브 지식 창고를 관리 및 파악하는 RAG 봇이다.
아래의 [제공된 내부 지식 문맥] 가이드라인만을 엄격하게 참고하여 사용자의 [질문]에 답변하라.
문맥에 단서가 전혀 없다면, 억지로 지어내지 말고 '원드라이브 장부에 관련 지식이 파악되지 않습니다'라고만 단호하게 선을 그어라.

[제공된 내부 지식 문맥]:
{context_str}

[질문]:
{user_question}

[답변]:"""

    try:
        ollama_response = requests.post(OLLAMA_URL, json={"model": OLLAMA_MODEL_NAME, "prompt": rag_prompt, "stream": False}, timeout=300)
        ai_response_text = ollama_response.json().get("response", "")
        
        if "원드라이브 장부에 관련 지식이 파악되지 않습니다" in ai_response_text:
            knowledge_found = False
            
        return ai_response_text, knowledge_found
    except Exception as e:
        return f"로컬 Ollama 엔진 통신 실패: {str(e)}", False


@slack_app.event("app_mention")
def handle_mention_events(event, say):
    raw_text = event.get("text", "")
    user_question = raw_text.split(">")[-1].strip()
    channel_id = event.get("channel")
    thread_ts = event.get("ts")
    parent_thread_ts = event.get("thread_ts")
    
    if parent_thread_ts and any(kw in user_question for kw in ["업로드", "승인"]):
        print("[ACTION] 원드라이브 업로드 수동 승인 신호 감지")
        try:
            replies = slack_app.client.conversations_replies(channel=channel_id, ts=parent_thread_ts)
            messages = replies.get("messages", [])
            
            report_text = ""
            for msg in reversed(messages):
                if msg.get("bot_id"):
                    text = msg.get("text", "")
                    if "로컬 RAG 파이프라인 가동 중" not in text and "원드라이브에 업로드할까요" not in text and text:
                        report_text = text
                        break
            
            if report_text:
                filename = "RAG_Knowledge_Report.md"
                encoded_content = report_text.encode('utf-8')
                access_token = get_fresh_access_token()
                
                if access_token:
                    upload_url = f"https://graph.microsoft.com/v1.0/me/drive/root:/{TARGET_FOLDER}/{filename}:/content"
                    headers = {
                        "Authorization": f"Bearer {access_token}",
                        "Content-Type": "text/plain; charset=utf-8"
                    }
                    up_res = requests.put(upload_url, headers=headers, data=encoded_content)
                    
                    if up_res.status_code in [200, 201]:
                        print(f"[OneDrive Write Success] 원드라이브 {TARGET_FOLDER} 폴더에 {filename} 적재 완공")
                        say(text=f"[확인] 유저 승인에 따라 원드라이브 클라우드 백업을 완료했습니다: /{TARGET_FOLDER}/{filename}", thread_ts=thread_ts)
                    else:
                        print(f"[MS GRAPH REJECT] 원드라이브 거부 사유: {up_res.text}")
                        say(text="[오류] 원드라이브 저장소 서버가 업로드를 거부했습니다.", thread_ts=thread_ts)
                else:
                    print("[AUTH REJECT] MS Graph 토큰 확보 실패")
                    say(text="[오류] 토큰 인증 실패로 원드라이브에 접근할 수 없습니다.", thread_ts=thread_ts)
            else:
                say(text="[경고] 업로드할 대상을 스레드 내에서 찾지 못했습니다.", thread_ts=thread_ts)
        except Exception as ms_err:
            print(f"[ONEDRIVE CRITICAL] 원드라이브 아카이빙 실패 사유: {str(ms_err)}")
            say(text=f"[오류] 원드라이브 연동 중 인프라 에러 발생: {str(ms_err)}", thread_ts=thread_ts)
        return

    if not user_question:
        say(text="질문 내용이 확인되지 않습니다. 봇을 멘션한 뒤 질문을 적어주세요!", thread_ts=thread_ts)
        return
        
    print(f"[Slack Ingress] 슬랙 채널로부터 질문 접수 완료: '{user_question}'")
    
    want_file_generation = any(kw in user_question for kw in ["파일", "다운로드", "생성", "리포트", "만들어"])
    
    say(text="로컬 RAG 파이프라인 가동 중... 잠시만 기다려주세요 (Ollama 추론 중)", thread_ts=thread_ts)
    
    ai_answer, knowledge_found = core_rag_pipeline(user_question)
    say(text=ai_answer, thread_ts=thread_ts)
    
    if want_file_generation:
        if not knowledge_found:
            print("[SKIP] 검색된 지식이 없으므로 파일 생성 루프를 차단합니다.")
            say(text="[안내] 매칭되는 사내 지식 기반이 존재하지 않아 다운로드 파일 및 승인 절차를 생성하지 않습니다.", thread_ts=thread_ts)
            return

        print("[ACTION] 지식 기반 파일 신규 생성 및 슬랙 첨부 가동")
        filename = "RAG_Knowledge_Report.md"
        encoded_content = ai_answer.encode('utf-8')
        file_buffer = io.BytesIO(encoded_content)
        
        try:
            slack_app.client.files_upload_v2(
                channel=channel_id,
                thread_ts=thread_ts,
                title="RAG 인프라 분석 신규 리포트",
                filename=filename,
                file=file_buffer
            )
            print("[Slack Egress] 다운로드용 파일 슬랙 적재 완료")
            
            say(
                text=f"생성된 리포트를 원드라이브(/{TARGET_FOLDER})에 추가로 업로드할까요? 업로드를 승인하시려면 이 댓글의 스레드에 다시 봇을 멘션하여 '업로드' 또는 '승인'이라고 입력해주세요.",
                thread_ts=thread_ts
            )
        except Exception as file_err:
            print(f"[SLACK FILE ERROR] 슬랙 업로드 실패: {str(file_err)}")


if __name__ == "__main__":
    if not SLACK_BOT_TOKEN or not SLACK_APP_TOKEN:
        print("[CRITICAL CODE ERROR] 슬랙 자격 증명 환경 변수가 선언되지 않았습니다.")
        exit(1)
        
    print("[Socket Mode Engine] 노트북 내부 하이브리드 RAG 슬랙 봇 가동 시작...")
    handler = SocketModeHandler(slack_app, SLACK_APP_TOKEN)
    handler.start()