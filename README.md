
## MongoDB 부하테스트 툴
### usage
- test : read write delete 테스트
- populate : 부하테스트
- clean : 청소
> python [test|populate|clean] [limit]

- `term`시간(초)에 한번씩 시간값 insert 쿼리
> python connect_test [term] 