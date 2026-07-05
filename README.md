OneDrive 관리 매니저

총 비용은 제 사용량 기준(소스 원드라이브 디렉토리 크기 532mb) S3 1.83 달러, bedrock 1.75 달러 청구 되었고 사용량에 따라 다를 수 있습니다.

1. Tech Stacks
	- Language: Python 3.13
	- AWS Cloud: Lambda, S3, EventBridge, IAM
	- AI & Vector DB: AWS Bedrock (amazon.titan-embed-text-v2:0), Amazon S3 Vectors
	- Integrations: Microsoft Graph API (OAuth 2.0 Webhook, Delta Sync Pipeline)
	- Concurrency: Python Concurrent Futures (ThreadPoolExecutor)

2. 사전 준비
	- MS onedrive
	- lambda 에 등록할 환경변수 구하기
		- MS_CLIENT_ID, MS_CLIENT_SECRET, MS_REFRESH_TOKEN, MS_SUBSCRIPTION_ID, MS_TARGET_FOLDER, MS_TENANT_ID, MS_USER_IDENTIFIER, REGION, VECTOR_BUCKET_NAME
	- OLLAMA 설치 및 AI 모델 설치(Ex. gemma4:e2b)
	- MS Azure 인프라 구성(하기 기술)
	- AWS 인프라 구성(하기 기술)
	- Slack 연동
	
3. 사용 방법
	1. local_ai.py 파일 실행(Ex. powershell 에서 실행 : python3.13.exe .\gemma_rag.py)
	2. 슬랙에서 앱 추가
	3. LLM을 사용할 채널 생성, 추가 된 앱 초대
	4. 초대한 앱에 태그 후 질문
	
	
2. 사전 준비 - Azure Infra 구성


본 파이프라인을 가동하기 전, Azure Portal에서 아래의 4가지 인프라 사전 설정 및 자격 증명 발급이 완료되어야 합니다.

Step 1. Microsoft Entra ID 앱 등록 (App Registration)
1. Azure Portal에 로그인한 뒤 Microsoft Entra ID 서비스로 이동합니다.
2. 왼쪽 메뉴에서 [앱 등록 (App Registrations)] ➔ [새 등록 (New Registration)]을 클릭합니다.
3. 앱 이름(예: `RAG-OneDrive-Ingestion`)을 입력하고, 지원되는 계정 유형을 [모든 조직 디렉터리의 계정 및 개인 Microsoft 계정]으로 선택한 뒤 등록합니다.
4. 등록 완료 후 화면에 표시되는 애플리케이션(클라이언트) ID를 별도로 기록해 둡니다. (Lambda의 MS_CLIENT_ID 환경 변수로 사용)

Step 2. 클라이언트 암호(Client Secret) 발급
1. 등록한 앱의 왼쪽 메뉴에서 [인증서 및 암호 (Certificates & Secrets)] ➔ [클라이언트 암호] 탭으로 이동합니다.
2. [새 클라이언트 암호 (New Client Secret)]를 클릭하고 만료 기간을 설정한 뒤 추가합니다.
3. 암호가 생성된 직후 화면에 보이는 값 (Value) 문자열을 즉시 복사하여 안전한 곳에 저장하세요. 페이지를 새로고침하면 다시는 확인하실 수 없습니다. (Lambda의 MS_CLIENT_SECRET 환경 변수로 사용)

Step 3. Microsoft Graph API 최소 권한 및 관리자 동의 부여
1. 앱의 왼쪽 메뉴에서 [API 권한 (API Permissions)] ➔ [권한 추가 (Add a permission)]를 클릭합니다.
2. [Microsoft Graph] ➔ [위임된 권한 (Delegated permissions)]을 선택합니다.
3. 아래 2가지 필수 권한 스코프를 검색하여 체크하고 추가합니다:
   - Files.ReadWrite (원드라이브 파일 스트림 인출용)
   - offline_access (백그라운드 Refresh Token 발급 및 토큰 자급자족용)
4. 권한 추가 후, 권한 목록 상단에 있는 [~에 대한 관리자 동의 허용 (Grant admin consent for ~)] 버튼을 반드시 클릭하여 상태를 녹색 체크 표시로 활성화해 줍니다.

Step 4. OneDrive 대상 감시 폴더 생성
1. 본인 계정의 OneDrive 루트 경로로 이동합니다.
2. 파이프라인이 실시간 감시할 전용 폴더인 AI_Agent_Docs 폴더를 생성합니다. 
   *(폴더명을 다르게 지정할 경우, AWS Lambda의 `MS_TARGET_FOLDER` 환경 변수 값을 해당 폴더명과 일치시켜야 합니다.)*

Step 5. 초기 부트스트랩 토큰(Initial Refresh Token) 적재
AWS Lambda는 서버리스 환경이므로 최초 1회는 사용자의 브라우저 인증을 통해 유효한 Refresh Token을 확보해야 합니다.
1. OAuth 2.0 인가 코드 플로우(Authorization Code Flow)를 통해 첫 인증을 진행하고 리프레시 토큰을 발급받습니다.
2. 발급받은 첫 토큰 문자열을 날것 그대로 ms_graph_refresh_token.txt라는 파일명으로 저장합니다.
3. 해당 파일을 유저님의 일반 S3 범용 버킷(`BUCKET_STATE`)의 루트 경로에 업로드해 둡니다. 이후부터는 Lambda가 이 토큰을 릴레이하며 스스로 갱신합니다.


2. 사전 준비 - AWS Infra 구성


Step 1. 일반 S3 버킷 생성 (상태 및 카탈로그 관리용)
1. AWS S3 콘솔에서 일반 목적(Standard) S3 버킷을 생성합니다. (예: my-rag-state-bucket)
2. 이 버킷은 Graph API의 동기화 책갈피(ms_graph_delta_link.txt) 및 마스터 장부(master_file_catalog.json)를 보관하는 상태 저장소로 활용됩니다.
3. 코드 상단의 BUCKET_STATE = "버킷명" 변수에 이 버킷 이름을 매핑합니다.

Step 2. Amazon S3 Vectors 버킷 및 인덱스 구축 (Vector DB)
1. AWS S3 Vectors 서비스 콘솔로 이동하여 전용 벡터 버킷을 생성합니다. (예: notebook-vector)
2. 해당 버킷 내에 벡터 인덱스를 아래 규격으로 생성합니다:
   - 인덱스 이름 (Index Name): 벡터 버킷의 인덱스 이름 (Lambda의 MS_INDEX_NAME 환경 변수와 매핑)
   - 데이터 유형 (Data Type): float32
   - 벡터 차원 (Dimension): 1024 (Bedrock Titan Embedding V2 규격과 동기화)
   - 거리 연산 메트릭 (Distance Metric): cosine (코사인 유사도 연산)
3. 코드 상단의 BUCKET_VECTOR = "벡터_버킷명" 변수에 이 버킷 이름을 매핑합니다.

Step 3. Amazon Bedrock 모델 권한 활성화 (Model Access)
1. AWS Bedrock 콘솔 ➔ 왼쪽 하단 [Model access] 메뉴로 이동합니다.
2. [Modify model access] 를 클릭한 후, Titan Text Embeddings V2 모델을 체크하고 권한을 활성화(Granted 상태 확인)합니다.

Step 4. AWS Lambda 함수 생성 및 IAM 실행 역할(Role) 설정
1. 런타임 환경: Python 3.13 환경으로 람다 함수를 생성합니다.
2. 제한 시간 및 스펙: 멀티스레딩 고속 청킹 및 대용량 파싱 처리를 위해 기본 제한 시간을 15분(최대)으로 확장하고, 메모리를 1024MB 이상으로 넉넉하게 할당합니다.
3. IAM 최소 권한 정책(Least Privilege Policy): 람다의 실행 역할(Execution Role)에 아래의 IAM 인라인 정책을 추가하여 일반 S3, S3 Vectors, Bedrock 런타임에 전권 권한을 부여합니다.

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject"
            ],
            "Resource": "arn:aws:s3:::범용_S3_버킷명/*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3vectors:PutVectors",
                "s3vectors:QueryVectors",
                "s3vectors:GetIndex"
            ],
            "Resource": "arn:aws:s3vectors:ap-northeast-2:AWS_계정ID:bucket/벡터_버킷명/*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "bedrock:InvokeModel"
            ],
            "Resource": "arn:aws:bedrock:ap-northeast-2::foundation-model/amazon.titan-embed-text-v2:0"
        },
        {
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents"
            ],
            "Resource": "arn:aws:logs:*:*:*"
        }
    ]
}
```

Step 4. AWS Lambda 함수 생성 및 IAM 실행 역할(Role) 설정
1. MS Graph API 웹훅의 3일 수명 만료 바리케이드를 방어하기 위해, EventBridge Scheduler를 단독 배포합니다.

2. Amazon EventBridge 콘솔 ➔ 왼쪽 사이드바 메뉴에서 [Scheduler] ➔ [일정 (Schedules)] ➔ [일정 생성 (Create schedule)]으로 진입합니다.

	일정 세부 정보 지정 (Define schedule detail):

	일정 이름 (Schedule name): Ex. rag-webhook-lifetime-extender

	일정 그룹 (Schedule group): default

	일정 패턴 (Schedule pattern): [되풀이되는 일정 (Recurring schedule)] 선택

	시간대 (Timezone): Asia/Seoul 또는 UTC 설정 (EventBridge Scheduler는 고유 타임존 지정을 지원하므로 국가 간 시차 연산 오류를 원천 차단합니다.)

	일정 유형: [Cron 기반 일정] 선택 후 아래 크론식 입력
		- cron(0 0 ? * MON,THU,SAT *)
	
3. 대상 선택 (Select target):

자주 사용되는 템플릿 대상 명단에서 [AWS Lambda]를 지정하고, 앞서 생성한 RAG 파이프라인 람다 함수를 매핑합니다.

4. 설정 및 실행 역할 (Flexible Execution Role):

Scheduler가 람다 함수를 직접 Invoke(호출)할 수 있도록 권한 정책 가이드라인에 따라 [새 IAM 역할 생성]을 선택하여 자동 권한 위임을 완성합니다.

동작 메커니즘: 지정된 크론 타임에 Scheduler가 람다를 호출하면, 람다 핸들러는 인입된 호출 컨텍스트(event.get("source") == "aws.events")를 감지하여 유저 개입 없이 백그라운드에서 Azure OAuth 서버와 통신해 웹훅 수명을 갱신합니다.


2. 사전 준비 - Slack 연동

노트북 로컬 가동 환경(Ollama Gemma4)과 슬랙 공식 서버 간에 Ngrok 같은 별도 터널링 도구 없이 보안 웹소켓을 연결하기 위한 슬랙 앱 설정입니다.

1. Slack API 콘솔 진입: [Slack App Dashboard](https://api.slack.com/apps)로 이동하여 [Create New App] ➔ [From scratch]를 클릭하고 앱 이름(예: `RAG-BOT`)과 대상 워크스페이스를 지정합니다.
2. 소켓 모드(Socket Mode) 개통:
   - 왼쪽 메뉴에서 [Socket Mode]로 이동하여 Enable Socket Mode* 토글 스위치를 On으로 켭니다.
   - 토글을 켜면 앱 레벨 토큰(App-level Token) 발급 창이 뜹니다. 토큰 이름(예: xapp_token)을 적고 connections:write 스코프가 포함된 것을 확인한 뒤 생성합니다.
   - 발급된 xapp- 으로 시작하는 토큰을 따로 기록해 둡니다. (챗봇의 `SLACK_APP_TOKEN` 환경 변수로 사용)
3. 앱 맨션 이벤트 구독 (Event Subscriptions):
   - 왼쪽 메뉴에서 [Event Subscriptions]로 이동하여 Enable Events를 On 으로 전환합니다.
   - 중간의 [Subscribe to bot events] 섹션을 펼치고 app_mention 이벤트를 추가합니다.
   - 설정을 마친 후 화면 맨 오른쪽 아래의 녹색 [Save Changes] 버튼을 눌러 슬랙 장부에 최종 저장합니다.
4. 봇 권한(Scopes) 검문 및 봇 토큰 발급:
   - 왼쪽 메뉴에서 [OAuth & Permissions]로 이동합니다.
   - 하단의 [Scopes] ➔ [Bot Token Scopes] 구역에 app_mentions:read (앞서 추가하여 자동 반영됨)와 슬랙방에 스레드로 댓글을 달 수 있도록 chat:write 권한이 인라인 정책으로 들어가 있는지 확인 후 없다면 수동 추가합니다.
   - 페이지 맨 위로 올라가 [Install to Workspace] 버튼을 클릭하고 권한을 [허용(Allow)] 합니다.
   - 발급된 xoxb- 로 시작하는 Bot User OAuth Token 을 기록해 둡니다. (챗봇의 SLACK_BOT_TOKEN 환경 변수로 사용)
5. 동작 및 UX 가드레일 최종 동기화:
   - 이벤트 구독이나 권한 스코프를 수정한 후에는 항상 왼쪽 메뉴 [Install App] 으로 이동하여 노란색 경고 창과 함께 활성화된 [Reinstall to Workspace] 버튼을 꾹 눌러 워크스페이스에 최종 안테나 동기화를 완료해 주어야 라우팅이 정상 작동합니다.