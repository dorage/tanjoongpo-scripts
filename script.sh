#!/bin/bash

set -eo pipefail

touch $log_filename
log_filename="$today.log"

function echo() {
	msg=$1
	timestamp=$(date '+%Y-%m-%d %H:%M:%S')

	echo "[$timestamp] $msg" >> $log_filename
}

yesterday="$( date +%Y )$( date +%m )$(($(date +%d) - 1))"
today="$( date +%Y )$( date +%m )$( date +%d )"

mbr_filename="${today}_001_mobile_mtchg.json"
filename="mbr_$today.json"

echo "탄중포 오늘의 작업 시작"

# mbr 데이터만 추출
cat $file | jq '.result | .mbr' > "$filename"

echo "DUCKDB: MBR 테이블 생성 시작"

# mbrs 데이터를 db로 생성
duckdb $today.duckdb "CREATE TABLE mbrs AS SELECT * FROM read_json('$filename')"

echo "DUCKDB: MBR 테이블 생성 완료"
echo "DUCKDB: ACR 테이블 생성 시작"

# acrs 테이블 생성
duckdb $today.duckdb "CREATE TABLE acrs (mtchgId varchar(255), incntvCd varchar(255), acrsCo int, dtlAcrs int)"

echo "DUCKDB: ACR 테이블 생성 완료"
echo "DUCKDB: 프로덕션 MYSQL 연결 시작"

# mysql 데이터 끌어오기 위해 연결
duckdb $today.duckdb "INSTALL mysql"
duckdb $today.duckdb "LOAD mysql"
duckdb $today.duckdb "ATTACH 'host=175.126.82.217 port=3306 user=readonly password=Kj7#mN9@pQ database=circularlabs' AS mysqldb (TYPE mysql)"
duckdb $today.duckdb "USE mysqldb"

echo "DUCKDB: 프로덕션 MYSQL 연결 완료"
echo "DUCKDB: ACR 테이블에 데이터 생성 시작"

# acrs 데이터 쌓기
duckdb $today.duckdb "
INSERT INTO db.acrs (mtchgId, incntvCd, acrsCo, dtlAcrs)
SELECT m.mtchgId as mtchgId, 'I0003' as incntvCd, r.count as acrsCo, 0 as dtlAcrs FROM mysql_query(
	'mysqldb', 
	'SELECT FN_DEC(r.user_name) as name, FN_DEC(r.user_phone) as phone, count(*) as count
	FROM rental r
	WHERE DATE(r.created_at)='$today'
	GROUP BY r.user_name, r.user_phone'
) r
LEFT JOIN db.mbrs m ON m.usernm=r.name and m.moblphon=r.phone"

echo "DUCKDB: ACR 테이블에 데이터 생성 완료"
echo "DUCKDB: ACR 테이블 JSON으로 내보내기 시작"

# acrs json으로 생성
duckdb $today.duckdb "COPY db.acrs TO '$today-001-E0960.json'"

echo "DUCKDB: ACR 테이블 JSON으로 내보내기 완료"
echo "오늘의 탄중포 작업 완료"
