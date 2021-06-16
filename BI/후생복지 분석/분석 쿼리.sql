수원시 후생복지 분석

## 전체 데이터 조회 ##
# common_data 1
| typecast SERIAL_NUMBER text
| typecast PARTICIPATION_YEAR text
| concat "0", BIRTHDAY
| substr concated -4 4 as BIRTH
| fillna TOUR_PLACE 미기재
| fields SERIAL_NUMBER as 연번, TYPE as 구분, NAME as 이름, CLASS_OF_POSITION as 직급, BIRTH as 생일, BUSINESS_NAME as 사업명, PARTICIPATION_YEAR as 참여연도, TOUR_PLACE as 여행지

# common_data 8
| typecast PARTICIPATION_YEAR text
| sort PARTICIPATION_YEAR

# common_data 10
* | where PARTICIPATION_YEAR = '${combo_4}'
| typecast SERIAL_NUMBER text
| typecast PARTICIPATION_YEAR text
| concat "0", BIRTHDAY
| substr concated -4 4 as BIRTH
| fillna TOUR_PLACE 미기재
| fields SERIAL_NUMBER as 연번, TYPE as 구분, NAME as 이름, CLASS_OF_POSITION as 직급, BIRTH as 생일, BUSINESS_NAME as 사업명, PARTICIPATION_YEAR as 참여연도, TOUR_PLACE as 여행지

# common_data 9
* | where PARTICIPATION_YEAR = '${combo_4}'
| where BUSINESS_NAME = '${combo_5}'
| typecast SERIAL_NUMBER text
| typecast PARTICIPATION_YEAR text
| concat "0", BIRTHDAY
| substr concated -4 4 as BIRTH
| fillna TOUR_PLACE 미기재
| fields SERIAL_NUMBER as 연번, TYPE as 구분, NAME as 이름, CLASS_OF_POSITION as 직급, BIRTH as 생일, BUSINESS_NAME as 사업명, PARTICIPATION_YEAR as 참여연도, TOUR_PLACE as 여행지


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
| fields SERIAL_NUMBER as 연번, TYPE as 구분, NAME as 이름, CLASS_OF_POSITION as 직급, BIRTH as 생일, BUSINESS_NAME as 사업명, PARTICIPATION_YEAR as 참여연도, TOUR_PLACE as 여행지


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