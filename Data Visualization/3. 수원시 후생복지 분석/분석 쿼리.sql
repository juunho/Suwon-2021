수원시 후생복지 분석_조회

## 전체 데이터 조회 ##
# common_data 1
| typecast SERIAL_NUMBER text
| typecast PARTICIPATION_YEAR text
| concat "0", BIRTHDAY
| substr concated -4 4 as BIRTH
| fillna TOUR_PLACE 미기재
| fields SERIAL_NUMBER as 연번, TYPE as 구분, NAME as 이름, BIRTH as 생일, BUSINESS_NAME as 사업명, PARTICIPATION_YEAR as 참여연도, TOUR_PLACE as 여행지

# common_data 8
| sort PARTICIPATION_YEAR

# common_data 10
* | where PARTICIPATION_YEAR = '${combo_4}'
| typecast SERIAL_NUMBER text
| typecast PARTICIPATION_YEAR text
| concat "0", BIRTHDAY
| substr concated -4 4 as BIRTH
| fillna TOUR_PLACE 미기재
| fields SERIAL_NUMBER as 연번, TYPE as 구분, NAME as 이름, BIRTH as 생일, BUSINESS_NAME as 사업명, PARTICIPATION_YEAR as 참여연도, TOUR_PLACE as 여행지

# common_data 9
* | where PARTICIPATION_YEAR = '${combo_4}'
| where BUSINESS_NAME = '${combo_5}'
| typecast SERIAL_NUMBER text
| typecast PARTICIPATION_YEAR text
| concat "0", BIRTHDAY
| substr concated -4 4 as BIRTH
| fillna TOUR_PLACE 미기재
| fields SERIAL_NUMBER as 연번, TYPE as 구분, NAME as 이름, BIRTH as 생일, BUSINESS_NAME as 사업명, PARTICIPATION_YEAR as 참여연도, TOUR_PLACE as 여행지


## 빠른 검색 ##
# common_data 2
| sort NAME

# common_data 3
* | where NAME = '${combo_2}'

# common_data 6
* | where NAME = '${combo_2}'
| where BIRTHDAY ='${combo_3}'
| concat "0", BIRTHDAY
| substr concated -4 4 as BIRTH
| typecast SERIAL_NUMBER text
| typecast PARTICIPATION_YEAR text
| fillna TOUR_PLACE 미기재
| sort PARTICIPATION_YEAR
| fields SERIAL_NUMBER as 연번, TYPE as 구분, NAME as 이름 , BIRTH as 생일, BUSINESS_NAME as 사업명, PARTICIPATION_YEAR as 참여연도, TOUR_PLACE as 여행지


## 세부 검색 ##
# common_data 7
* | command cache clear

# common_data 4
| concat "0", BIRTHDAY
| substr concated -4 4 as BIRTH
| concat NAME, "_", BIRTH as ID
| sort ID
| fillna TYPE '미기재', BUSINESS_NAME '미기재', PARTICIPATION_YEAR '미기재'
| fields ID as ID (이름_생일), TYPE as 구분, BUSINESS_NAME as 희망 참여사업, PARTICIPATION_YEAR as 희망 참여연도

# common_data 5
| concat "0", BIRTHDAY
| substr concated -4 4 as BIRTH
| concat NAME, "_", BIRTH as ID 
| join outer '후생 참여자 ID' 후생 참여 희망자.ID = 후생 참여자 ID.ID 
| where ID is not null 
| concat 후생 참여자 ID_PARTICIPATION_YEAR, "_", 후생 참여자 ID_BUSINESS_NAME 
| fields ID, concated
| sort ID, concated 
| sql "select*, rank() over(order by ID) as DIS from angora" 
| numbering NUM 
| case when NUM = DIS then ' ' 
| concat ID, result as RID 
| fillna concated '처음 참여' , RID ' '
| fields RID as ID (이름_생일), concated as 이전 참여 목록


수원시 후생복지 분석_현황 (추가 내용)

## 후생복지 사업 현황 ##
# common_data 1
| case when BUSINESS_NAME = '힐링하우스' then '기타프로그램' when BUSINESS_NAME = '슬기로운 집콕생활' then '기타프로그램' when  BUSINESS_NAME = '힐링프로그램' then '기타프로그램' when BUSINESS_NAME = '하계휴양소' then '휴양지원' when BUSINESS_NAME = '휴양지원금' then '휴양지원' otherwise BUSINESS_NAME as BUSINESS_NAME 
| stats count(SERIAL_NUMBER) as CNT by PARTICIPATION_YEAR, BUSINESS_NAME
| pivot max(cnt) splitrow PARTICIPATION_YEAR splitcol BUSINESS_NAME sortrow ASC
| rename `PARTICIPATION_YEAR` `연도`
| case when `연도` = 2016 then ' (힐링프로그램)' when `연도` = 2017 then ' (힐링프로그램)' when `연도` = 2018 then ' (힐링프로그램)' when `연도` = 2019 then ' (힐링프로그램)' when `연도` = 2020 then ' (슬기로운 집콕생활)' when `연도` = 2021 then ' (힐링하우스)' as 기타내용 
| concat 기타프로그램, "\r", 기타내용 as 기타프로그램 
| fillna '국외문화탐방' 0,  '법인콘도(하계성수기)' 0, '공직맘공직빠아이즐거워' 0, '국내문화탐방' 0, '국내배낭연수' 0, '휴양지원' 0, '국외배낭연수' 0, '생태체험' 0
| join inner '후생복지 총계' as '' 연도 = .YEAR
| rename `_총 인원` `총 인원`, `_참가자 총계(비율)` `참가자 총계`
| sort 연도

# common_data 2
| stats count(*) as `연간 참여자` by PARTICIPATION_YEAR 
| sort PARTICIPATION_YEAR

# common_data 3
| stats count(*) as CNT by PARTICIPATION_YEAR 
| sort PARTICIPATION_YEAR
| sql "select*, sum(CNT) over(order by PARTICIPATION_YEAR) as `누적 참여자` from angora"


## 분류별 후생복지 참여 현황 ##
# common_data 4
| typecast  PARTICIPATION_YEAR INTEGER 
| stats count(*) as CNT by PARTICIPATION_YEAR, type 
| case when type = '공무원' then CNT as 공무원 
| case when type ='청원경찰' then CNT as 청원경찰
| case when type = '공무원' then CNT as 공무원 
| case when type = '공무직' then CNT as 공무직 
| case when type = '시의원' then CNT as 시의원
| case when type = '환경관리원' then CNT as 환경관리원 
| case when type = '예술단원' then CNT as 예술단원 
| sort  PARTICIPATION_YEAR

# common_data 5
| typecast  PARTICIPATION_YEAR INTEGER 
| where type = '공무원'
| Where CLASS_OF_POSITION != '기타'
| case when CLASS_OF_POSITION = '5급' then '5급 이상' when CLASS_OF_POSITION = '4급' then '5급 이상' when CLASS_OF_POSITION = '3급' then '5급 이상' when CLASS_OF_POSITION = '2급' then '5급 이상' when CLASS_OF_POSITION = '1급' then '5급 이상'otherwise CLASS_OF_POSITION as CLASS_OF_POSITION
| stats count(*) as CNT by PARTICIPATION_YEAR, CLASS_OF_POSITION  | case when CLASS_OF_POSITION = '9급' then CNT as `9급`
| case when CLASS_OF_POSITION = '8급' then CNT as `8급`
| case when CLASS_OF_POSITION = '7급' then CNT as `7급`
| case when CLASS_OF_POSITION = '6급' then CNT as `6급`
| case when CLASS_OF_POSITION = '5급 이상' then CNT as `5급 이상`

# common_data 6
| where type  = '공무원'
| stats count(*) as 참여 by PARTICIPATION_YEAR 
| join outer '수원시 직원 현황' 후생 복지 참여자.PARTICIPATION_YEAR = 수원시 직원 현황.STANDARD_YEAR 
| sort PARTICIPATION_YEAR 
| where `수원시 직원 현황_TYPE` = '공무원'  
| fields  PARTICIPATION_YEAR, 참여, 수원시 직원 현황_STANDARD_YEAR as YEAR, 수원시 직원 현황_NUMBER_OF_PERSON_COUNT as 전체 
| calculate 전체 - 참여 as 미참여 
| calculate 참여 / 전체*100 as 참여율 
| calculate 미참여 / 전체*100 as 미참여율 
| calculate 참여율 + 미참여율 as 전체비율 
| round 2 col = [미참여율, 참여율]
| fields year, 미참여율, 참여율

# common_data 7
| where type  = '공무직'
| stats count(*) as 참여 by PARTICIPATION_YEAR 
| join outer '수원시 직원 현황' 후생 복지 참여자.PARTICIPATION_YEAR = 수원시 직원 현황.STANDARD_YEAR 
| sort PARTICIPATION_YEAR  
| where `수원시 직원 현황_TYPE` = '공무직'  
| fields  PARTICIPATION_YEAR, 참여, 수원시 직원 현황_STANDARD_YEAR as YEAR, 수원시 직원 현황_NUMBER_OF_PERSON_COUNT as 전체 
| calculate 전체 - 참여 as 미참여 
| calculate 참여 / 전체*100 as 참여율 
| calculate 미참여 / 전체*100 as 미참여율 
| calculate 참여율 + 미참여율 as 전체비율 
| round 2 col = [미참여율, 참여율]
| fields year, 미참여율, 참여율

# common_data 8
| where type  = '공무원' 
| CONCAT "0", BIRTHDAY
| SUBSTR concated -4 4 as BIRTH
| concat NAME, "_", BIRTH as ID 
| stats count(*) as CNT by ID, PARTICIPATION_YEAR 
| case when CNT = 1 then '1회' otherwise '2회 이상' as 참여 
| stats sum(CNT) as 인원 by 참여, PARTICIPATION_YEAR 
| sort PARTICIPATION_YEAR , 참여 
| case when 참여 = '1회' then 인원 as `1회 참여` 
| case when 참여 = '2회 이상' then 인원 as `2회 이상 참여` 
| sql " select DISTINCT PARTICIPATION_YEAR, max(`1회 참여`) over(partition by PARTICIPATION_YEAR) as `1회 참여`, max(`2회 이상 참여`) over(partition by PARTICIPATION_YEAR)  as `2회 이상 참여` from angora" 
| join outer '수원시 직원 현황' 후생 복지 참여자.PARTICIPATION_YEAR = 수원시 직원 현황.STANDARD_YEAR
| sort PARTICIPATION_YEAR  
| where `수원시 직원 현황_TYPE` = '공무원' 
| calculate `1회 참여` + `2회 이상 참여` as `전체 참여` 
| calculate `수원시 직원 현황_NUMBER_OF_PERSON_COUNT` - `전체 참여` as 미참여
| fields PARTICIPATION_YEAR as YEAR, 1회 참여, 2회 이상 참여,  수원시 직원 현황_NUMBER_OF_PERSON_COUNT as 전체, 미참여

# common_data 9
| where type  = '공무직' 
| CONCAT "0", BIRTHDAY
| SUBSTR concated -4 4 as BIRTH
| concat NAME, "_", BIRTH as ID 
| stats count(*) as CNT by ID, PARTICIPATION_YEAR 
| case when CNT = 1 then '1회' otherwise '2회 이상' as 참여 
| stats sum(CNT) as 인원 by 참여, PARTICIPATION_YEAR 
| sort PARTICIPATION_YEAR , 참여 
| case when 참여 = '1회' then 인원 as `1회 참여` 
| case when 참여 = '2회 이상' then 인원 as `2회 이상 참여` 
| sql " select DISTINCT PARTICIPATION_YEAR, max(`1회 참여`) over(partition by PARTICIPATION_YEAR) as `1회 참여`, max(`2회 이상 참여`) over(partition by PARTICIPATION_YEAR)  as `2회 이상 참여` from angora" 
| join outer '수원시 직원 현황' 후생 복지 참여자.PARTICIPATION_YEAR = 수원시 직원 현황.STANDARD_YEAR
| sort PARTICIPATION_YEAR  
| where `수원시 직원 현황_TYPE` = '공무직' 
| calculate `1회 참여` + `2회 이상 참여` as `전체 참여` 
| calculate `수원시 직원 현황_NUMBER_OF_PERSON_COUNT` - `전체 참여` as 미참여
| fields PARTICIPATION_YEAR as YEAR, 1회 참여, 2회 이상 참여,  수원시 직원 현황_NUMBER_OF_PERSON_COUNT as 전체, 미참여

## 사업별 후생복지 참여 현황 ##
# common_data 10 ~ 21
| where BUSINESS_NAME  = '사업명' 
| stats sum(CNT) as CNT by TYPE, 순서

# common_data 22 ~ 64
| where PARTICIPATION_YEAR = 2016 ~ 2021
| where BUSINESS_NAME  = '사업명'