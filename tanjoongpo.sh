#!/bin/bash

set -eo pipefail

yesterday="$( date +%Y )$( date +%m )$(($(date +%d) - 1))"
today="$( date +%Y )$( date +%m )$( date +%d )"

log_filename="$today.log"
touch "$log_filename"

function log() {
	msg=$1
	timestamp=$(date '+%Y-%m-%d %H:%M:%S')

	echo "[$timestamp] $msg" >> $log_filename
}

log "탄중포 오늘의 작업 시작"

log "mbr_info 가져오기"
cp -r /home/sftpuser/mbr_info /home/ec2-user/mbr_info

log "acrs 디렉터리 생성"
mkdir acrs

company_code="E0960"
root="/home/ec2-user"
mbr_filename="$home/mbr_info/${today}_001_mobile_mtchg.json"
mbr_only_filename="$home/mbr_info/mbr_$today.json"
acr_filename="$home/acrs/$today-001-$company_code.json"
acr_only_filename="$home/acrs/acr_$today.json"

# mbr 데이터만 추출
cat $mbr_filename | jq '.result | .mbr' > "$mbr_only_filename"

log "DUCKDB: MBR 테이블 생성 시작"

# mbrs 데이터를 db로 생성
duckdb db.duckdb "CREATE TABLE mbrs AS SELECT * FROM read_json('$mbr_only_filename')"

log "DUCKDB: MBR 테이블 생성 완료"
log "DUCKDB: ACR 테이블 생성 시작"

# acrs 테이블 생성
duckdb db.duckdb "CREATE TABLE acrs (mtchgId varchar(255), incntvCd varchar(255), acrsCo int, dtlAcrs int)"

log "DUCKDB: ACR 테이블 생성 완료"
log "DUCKDB: 데이터 매칭 시작"

# mysql 데이터 끌어오기 위해 연결
duckdb db.duckdb "INSTALL mysql"
duckdb db.duckdb "LOAD mysql"

duckdb db.duckdb "
ATTACH 'host=175.126.82.217 port=3306 user=readonly password=Kj7#mN9@pQ database=circularlabs' AS mysqldb (TYPE mysql);
USE mysqldb;

INSERT INTO db.acrs (mtchgId, incntvCd, acrsCo, dtlAcrs)
SELECT m.mtchgId as mtchgId, 'I0003' as incntvCd, r.count as acrsCo, 0 as dtlAcrs FROM mysql_query(
	'mysqldb', 
	'SELECT FN_DEC(r.user_name) as name, FN_DEC(r.user_phone) as phone, count(*) as count
	FROM rental r
	WHERE DATE(r.created_at)=\"$( date +%Y )-$( date +%m )-$( date +%d )\"
	GROUP BY r.user_name, r.user_phone'
) r
LEFT JOIN db.mbrs m ON m.usernm=r.name and m.moblphon=r.phone
WHERE mtchgId IS NOT NULL
"

log "DUCKDB: 데이터 매칭 완료"
log "DUCKDB: ACR 테이블 JSON으로 내보내기 시작"

# acrs json으로 생성
duckdb db.duckdb "COPY db.acrs TO '$acr_only_filename' (FORMAT JSON, ARRAY true)"

log "DUCKDB: ACR 테이블 JSON으로 내보내기 완료"
log "오늘의 acrs 파일 생성"

echo "{
	\"info\": {
		\"requestNo\": \"001\",
		\"partcptnEntCd\": \"$company_code\",
		\"rlvtDe\": \"$today\",
		\"nextMthd\": \"F\",
		\"encptCd\": \"PT\",
		\"totcnt\": \"$( jq 'length' $acr_only_filename )\",
		\"errCd\": \"E000\"
	},
	\"acrs\": $( cat $acr_only_filename )
}" >> $acr_filename

log "오늘의 acrs 파일 이동"

cp $acr_filename /home/sftpuser/acrs


log "마무으리"
rm -rf "$root/mbr_info"
rm -rf "$root/acrs"
rm db.duckdb
