
요구사항
1. 로그인 기능
2. 채팅 방 생성 
3. 채팅은 방장이 한 명 있을 수 있다.
4. 부방장이 있을 수 있다.
5. 채팅 내역은 보존된다.
6. 계정이 있어야한다.
7. 




Q. 
1. 채팅 보존의 용량은 어떻게 줄이지? 
2. redis failover 
3. kafka 리밸런싱 대처
4. kafka produce 버퍼 프로듀서


기술
kafka 백본
redis sharded pub/sub
webflux
elastic search


phase2
로그인은 별도의 서버
채팅에서는 토큰으로만 검증
욕설과 같은 필터링 작업