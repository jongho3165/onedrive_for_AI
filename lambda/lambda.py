import json  # JSON 데이터 파싱 및 직렬화를 위한 기본 모듈 임포트
import io  # 메모리 내 바이너리 바이트 스트림(BytesIO) 처리를 위한 모듈 임포트
import os  # 람다 환경 변수(Environment Variables) 조회를 위한 모듈 임포트
import datetime  # 웹훅 연장 시간 계산(UTC 표준시)을 위한 날짜 모듈 임포트
import time  # 엇박자 지연 대기를 위한 시간 모듈 임포트
from concurrent.futures import ThreadPoolExecutor  # 속도 혁명을 위한 멀티스레딩 모듈 임포트
import boto3  # S3 및 Bedrock 등의 AWS 서비스를 제어하기 위한 SDK 임포트
import requests  # MS Graph API 및 OAuth 서버와 통신할 HTTP 라이브러리 임포트
from botocore.config import Config  # 커넥션 풀 크기 확장을 위한 모듈 임포트
 
# AWS labda 함수 실행 전 olefile, pdf_word 계층 람다에 추가 필요
# 람다에 환경변수 추가 필요(MS_CLIENT_ID, MS_CLIENT_SECRET, MS_REFRESH_TOKEN, MS_SUBSCRIPTION_ID, MS_TARGET_FOLDER, MS_TENANT_ID, MS_USER_IDENTIFIER, REGION, VECTOR_BUCKET_NAME)



#  [전 구간 관로 확장] 10개 동시 스레드가 대기 없이 질주하도록 S3/Bedrock 공통 풀 크기를 20으로 확장
aws_pipeline_config = Config(max_pool_connections=20)

# 1. 장부 관리를 위한 전통적인 일반 S3 오브젝트 클라이언트
s3 = boto3.client('s3', config=aws_pipeline_config)

# 2. 임베딩 벡터 추출을 위한 Bedrock 런타임 클라이언트
bedrock = boto3.client(
    'bedrock-runtime', 
    region_name='ap-northeast-2', 
    config=aws_pipeline_config
)

# 3. [AI 혁신 관로] Amazon S3 Vectors 전용 네이티브 클라이언트
s3vectors = boto3.client(
    's3vectors', 
    region_name='ap-northeast-2', 
    config=aws_pipeline_config
)

#  [인프라 이원화 버킷 매핑] 이름 문자열은 같지만 각각의 전용 클라이언트를 타고 찾아갑니다.
BUCKET_STATE = "" # 일반 S3 버킷 (장부 및 증서용)
BUCKET_VECTOR = "" # Amazon S3 Vectors 전용 벡터 버킷 이름

#  [인덱스 이름 완벽 동기화] 유저님이 생성해 두신 실제 인덱스 이름으로 고정
INDEX_NAME = os.environ.get("MS_INDEX_NAME", "") # 벡터 버킷 인덱스 이름

#  [보안 가드레일] 중요 기밀 정보는 람다 환경 변수에서 런타임에 땡겨옵니다.
CLIENT_ID = os.environ.get("MS_CLIENT_ID")
CLIENT_SECRET = os.environ.get("MS_CLIENT_SECRET")
SUBSCRIPTION_ID = os.environ.get("MS_SUBSCRIPTION_ID")

# [특정 경로 필터] 감시할 원드라이브 특정 폴더명
TARGET_FOLDER = os.environ.get("MS_TARGET_FOLDER", "").strip()

if TARGET_FOLDER:
    DEFAULT_DELTA_URL = f"https://graph.microsoft.com/v1.0/me/drive/root:/{TARGET_FOLDER}:/delta"
    print(f" [Configuration] 감시 타깃 폴더 고정 완료: 원드라이브:/root/{TARGET_FOLDER}")
else:
    DEFAULT_DELTA_URL = "https://graph.microsoft.com/v1.0/me/drive/root/delta"
    print(" [Configuration] 지정된 폴더가 없어 원드라이브 전체(Root)를 감시합니다.")

TOKEN_URL = "https://login.microsoftonline.com/common/oauth2/v2.0/token"


def get_fresh_access_token():
    token_obj = s3.get_object(Bucket=BUCKET_STATE, Key="ms_graph_refresh_token.txt")
    old_refresh_token = token_obj['Body'].read().decode('utf-8').strip()

    payload = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "refresh_token",
        "refresh_token": old_refresh_token,
        "scope": "files.readwrite offline_access"
    }

    token_response = requests.post(TOKEN_URL, data=payload).json()
    new_refresh_token = token_response.get("refresh_token")

    if new_refresh_token:
        s3.put_object(Bucket=BUCKET_STATE, Key="ms_graph_refresh_token.txt", Body=new_refresh_token.encode('utf-8'))

    return token_response.get("access_token")


#  [내진 설계 부품] Stream has ended unexpectedly 에러를 방어하는 안전 S3 파일 리더 함수
def safe_s3_catalog_read(bucket, key):
    max_s3_retries = 3
    for attempt in range(max_s3_retries):
        try:
            catalog_obj = s3.get_object(Bucket=bucket, Key=key)
            return json.loads(catalog_obj['Body'].read().decode('utf-8'))
        except Exception as s3_stream_err:
            if "Stream has ended unexpectedly" in str(s3_stream_err) and attempt < max_s3_retries - 1:
                print(f"⏳ [STREAM RETRY] S3 대장 스트림 단절 감지. {attempt + 1}회차 재연결선 가동 전 1.5초 대기...")
                time.sleep(1.5)
                continue
            raise s3_stream_err


#  [S3 Vectors 전용 병렬 워커] 단일 청크의 Bedrock 임베딩 변환 및 S3 Vectors 인덱스 다이렉트 주입
def process_single_chunk(index, chunk, file_id, file_name):
    try:
        bedrock_payload = {"inputText": chunk, "dimensions": 1024}
        bedrock_response = bedrock.invoke_model(
            body=json.dumps(bedrock_payload),
            modelId="amazon.titan-embed-text-v2:0",
            accept="application/json",
            contentType="application/json"
        )
        response_body = json.loads(bedrock_response.get("body").read())
        embedding_vector = response_body.get("embedding")

        vector_payload = [
            {
                'key': f"{file_id}_{index}",
                'data': {
                    'float32': embedding_vector
                },
                'metadata': {
                    'file_name': file_name,
                    'text_chunk': chunk
                }
            }
        ]
        
        s3vectors.put_vectors(
            vectorBucketName=BUCKET_VECTOR,
            indexName=INDEX_NAME,
            vectors=vector_payload
        )
    except Exception as e:
        print(f" [S3 VECTORS ERROR] 파일({file_name}) 청크 인덱스 {index}번 네이티브 주입 실패: {str(e)}")


def lambda_handler(event, context):
    try:
        # ======================================================================
        # [DevOps Hotfix] 레이어 구조 꼬임 강제 우회 가드레일 가동
        # ======================================================================
        import sys
        extra_paths = [
            '/opt/new_document_parsers',
            '/opt/new_document_parsers/python'
        ]
        for path in extra_paths:
            if os.path.exists(path) and path not in sys.path:
                sys.path.append(path)
                print(f" [Hotfix Inject] 라이브러리 탐색 경로 수동 연동 성공: {path}")
        
        print("[DEBUG] /opt 내부 폴더 실측:", os.listdir('/opt') if os.path.exists('/opt') else "경로 없음")
        if os.path.exists('/opt/python'):
            print("[DEBUG] /opt/python 내부 알맹이 실측:", os.listdir('/opt/python'))
        print(" [DEBUG] 현재 람다 sys.path 탐색 경로:", sys.path)
        # ======================================================================

        # ======================================================================
        # [트랙 A] AWS EventBridge 타이머가 람다를 깨운 경우 (수명 연장)
        # ======================================================================
        if event.get("source") == "aws.events":
            print("[Timer] 3일 만료 방지를 위한 MS 웹훅 계약 연장 시퀀스 작동 시작")
            access_token = get_fresh_access_token()
            renew_url = f"https://graph.microsoft.com/v1.0/subscriptions/{SUBSCRIPTION_ID}"
            
            headers = {
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json"
            }
            future_expire = (datetime.datetime.now(datetime.UTC) + datetime.timedelta(days=3)).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            renew_payload = {"expirationDateTime": future_expire}

            renew_response = requests.patch(renew_url, headers=headers, json=renew_payload)
            
            if renew_response.status_code == 200:
                print(f"[Success] 웹훅 구독 연장 성공! 새 만료일자: {future_expire}")
                return {"statusCode": 200, "body": "Subscription Successfully Extended"}
            else:
                print(f"[Fail] 연장 거부 사유: {renew_response.text}")
                return {"statusCode": renew_response.status_code, "body": "MS Denied Extension"}

        # ======================================================================
        # [트랙 B] MS OneDrive 웹훅 알림이 울려 들어온 경우 (실전 파일 처리)
        # ======================================================================
        validation_token = event.get("queryStringParameters", {}).get("validationToken")

        if validation_token:
            print("[Ingress] MS 웹훅 최초 등록용 유효성 검증 승인 패스 반사")
            return {"statusCode": 200, "body": validation_token}

        access_token = get_fresh_access_token()

        # ======================================================================
        # ⏱️ [대장 강제 갱신 가드레일] 웹훅 이벤트 진입 시 일반 S3 범용 버킷의 대장을 수정합니다.
        # ======================================================================
        try:
            catalog_data = safe_s3_catalog_read(BUCKET_STATE, "master_file_catalog.json")
        except s3.exceptions.NoSuchKey:
            catalog_data = {"total_files": 0, "files": []}

        catalog_data["last_pipeline_run"] = datetime.datetime.now(datetime.UTC).strftime('%Y-%m-%d %H:%M:%S UTC')

        s3.put_object(
            Bucket=BUCKET_STATE,
            Key="master_file_catalog.json",
            Body=json.dumps(catalog_data, ensure_ascii=False, indent=2).encode('utf-8')
        )
        print(f"⏱️ [Health Check] 일반 범용 S3 버킷 대장 파일 타임스탬프 갱신 완료.")
        # ======================================================================

        try:
            delta_obj = s3.get_object(Bucket=BUCKET_STATE, Key="ms_graph_delta_link.txt")
            delta_url = delta_obj['Body'].read().decode('utf-8').strip()
            print(f"[Resume] 기존 실시간 체크포인트 책갈피 인출 성공. 이어서 진도 뺍니다.")
        except s3.exceptions.NoSuchKey:
            delta_url = DEFAULT_DELTA_URL
            print(f"[Full Scan] 책갈피 기록이 없어 태초의 상태부터 전수조사를 가동합니다.")

        headers = {"Authorization": f"Bearer {access_token}"}

        while True:
            print(f"[Request] 델타 관로 타격 중: {delta_url[:60]}...")
            delta_response = requests.get(delta_url, headers=headers).json()
            
            if "error" in delta_response and delta_response["error"].get("code") == "resyncRequired":
                print("[Auto-Resync] S3 책갈피 토큰 만료 감지. 지정 경로 초기 주소로 자동 재동기화 격발.")
                delta_url = DEFAULT_DELTA_URL
                continue

            changed_items = delta_response.get("value", [])
            print(f"[Batch] 이번 페이지에서 감지된 변동 자산 수: {len(changed_items)}개")
            
            for item in changed_items:
                file_name = item.get("name", "")
                file_id = item.get("id", "")
                
                print(f"[TRAFFIC] 탐지된 파일명: {file_name} | 확장자: {file_name.split('.')[-1].lower() if '.' in file_name else 'none'}")

                if "folder" in item or "deleted" in item:
                    print(f"[SKIP] 폴더 구조 또는 삭제 이벤트를 건너뜀: {file_name}")
                    continue

                download_url = item.get("@microsoft.graph.downloadUrl")
                if not download_url:
                    if file_id:
                        download_url = f"https://graph.microsoft.com/v1.0/me/drive/items/{file_id}/content"
                        print(f"[REDIRECT] 전수조사 기점 링크 공백 확인 -> 공식 API 관로 강제 맵핑 연동: {file_name}")
                    else:
                        continue

                if file_name.startswith("[요약]") or "chunk" in file_name:
                    print(f"[GUARD] 무한 루프 차단: 람다 자가 생성 파일({file_name}) 패싱 완료")
                    continue

                file_response = requests.get(download_url, headers=headers, stream=True)
                file_bytes = file_response.content
                ext = file_name.split('.')[-1].lower() if '.' in file_name else ''
                extracted_text = ""

                # 다중 파서 엔진 라우팅 구역
                if ext in ['txt', 'md']:
                    extracted_text = file_bytes.decode('utf-8')
                elif ext == 'pdf':
                    from pypdf import PdfReader
                    pdf_stream = io.BytesIO(file_bytes)
                    reader = PdfReader(pdf_stream)
                    extracted_text = "\n".join([page.extract_text() for page in reader.pages if page.extract_text()])
                elif ext == 'docx':
                    from docx import Document
                    docx_stream = io.BytesIO(file_bytes)
                    doc = Document(docx_stream)
                    extracted_text = "\n".join([p.text for p in doc.paragraphs])
                elif ext == 'hwpx':
                    import zipfile
                    import xml.etree.ElementTree as ET
                    hwpx_stream = io.BytesIO(file_bytes)
                    with zipfile.ZipFile(hwpx_stream) as z:
                        section_files = [f for f in z.namelist() if "Contents/section" in f and f.endswith(".xml")]
                        text_list = []
                        for s_file in sorted(section_files):
                            xml_content = z.read(s_file)
                            root = ET.fromstring(xml_content)
                            text_list.append("".join(root.itertext()))
                        extracted_text = "\n".join(text_list)
                elif ext == 'hwp':
                    import olefile
                    hwp_stream = io.BytesIO(file_bytes)
                    
                    if olefile.isOleFile(hwp_stream):
                        try:
                            ole = olefile.OleFileIO(hwp_stream)
                            if ole.exists('BodyText/Section0'):
                                data = ole.openstream('BodyText/Section0').read()
                                extracted_text = data.decode('utf-16', errors='ignore')
                            else:
                                print(f"[PARSER WARN] '{file_name}' 본문 Section0 스트림이 유실되었습니다.")
                        except Exception as hwp_inner_err:
                            print(f"[PARSER ERROR] '{file_name}' olefile 디코딩 실패 사유: {str(hwp_inner_err)}")
                            continue
                    else:
                        print(f"[FAULT ISOLATION] '{file_name}' 확장자는 hwp이나 실제 OLE2 구조가 아닙니다. 스킵 조치.")
                        continue

                print(f"[PARSER] 파일명: {file_name} | 추출된 최종 평문 글자수: {len(extracted_text.strip())}자")

                if not extracted_text.strip():
                    print(f"[SKIP 2] 파싱된 내용이 없는 공백 문서이므로 대장 기입 패스: {file_name}")
                    continue

                print(f"[LEDGER] 대장 기입 시작선 통과 (정상 지식 인덱싱 처리 대상): {file_name}")

                # 400자 고정 청킹 분쇄
                chunk_size = 400
                chunks = [extracted_text[i:i + chunk_size] for i in range(0, len(extracted_text), chunk_size)]

                # ⚡ [밸런스 형 병렬 가동] 최대 10개 병렬 스레드로 임베딩 안정성 극대화 확보
                print(f"[CONCURRENCY] '{file_name}' 파일 분쇄 시작: 총 {len(chunks)}개 청크 병렬 처리 가동")
                with ThreadPoolExecutor(max_workers=10) as executor:
                    for index, chunk in enumerate(chunks):
                        executor.submit(process_single_chunk, index, chunk, file_id, file_name)

                #  [내진 설계 반영] 일반 S3 범용 버킷 내 마스터 카탈로그 대장 읽기 자동 복구 시퀀스 작동
                try:
                    catalog_data = safe_s3_catalog_read(BUCKET_STATE, "master_file_catalog.json")
                except s3.exceptions.NoSuchKey:
                    catalog_data = {"total_files": 0, "files": []}

                catalog_data["files"] = [f for f in catalog_data["files"] if f["path"] != file_name]

                new_log = {
                    "path": file_name,
                    "time": item.get("lastModifiedDateTime", ""),
                    "raw_time": int(datetime.datetime.now(datetime.UTC).timestamp())
                }
                catalog_data["files"].append(new_log)
                catalog_data["total_files"] = len(catalog_data["files"])
                catalog_data["files"] = sorted(catalog_data["files"], key=lambda x: x["raw_time"], reverse=True)

                s3.put_object(
                    Bucket=BUCKET_STATE,
                    Key="master_file_catalog.json",
                    Body=json.dumps(catalog_data, ensure_ascii=False, indent=2).encode('utf-8')
                )
                print(f"[SUCCESS] master_file_catalog.json 장부에 '{file_name}' 업데이트 정상 완공!")

            # 페이지네이션 마감 검문소 및 실시간 체크포인트 저장소
            next_link = delta_response.get("@odata.nextLink")
            delta_link = delta_response.get("@odata.deltaLink")

            if next_link:
                print("[Checkpoint] 다음 페이지 감지. 타임아웃 대비 실시간 이어받기 책갈피 S3 적재 집행.")
                s3.put_object(Bucket=BUCKET_STATE, Key="ms_graph_delta_link.txt", Body=next_link.encode('utf-8'))
                delta_url = next_link
                
                # [자진 퇴근 가드레일] 남은 시간이 60초 미만이면 다음 대용량 페이지 처리를 생략하고 우아하게 정상 종료
                if context.get_remaining_time_in_millis() < 60000:
                    print(f"[Time Over Guard] 람다 제한 시간(15분)이 1분 미만으로 남았습니다. 데이터 유실 없이 여기서 1차 정상 자진 퇴근합니다.")
                    return {"statusCode": 200, "body": json.dumps("Page processed successfully. Checkpoint saved. Run again for next page.")}
                    
            elif delta_link:
                print(f"[Horizon] 최종 목적지 도달 완료. 최종 마감 동기화 책갈피 S3 적재 집행.")
                s3.put_object(Bucket=BUCKET_STATE, Key="ms_graph_delta_link.txt", Body=delta_link.encode('utf-8'))
                break
            else:
                break

        return {"statusCode": 200, "body": json.dumps("RAG Ingestion & Webhook Logic Successfully Processed")}

    except Exception as e:
        print(f"[CRITICAL ERROR] 파이프라인 엔진 다운 사유: {str(e)}")
        return {"statusCode": 500, "body": json.dumps(f"Lambda Pipeline Error: {str(e)}")}