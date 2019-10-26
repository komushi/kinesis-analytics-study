## 1. Get Data
http://nlftp.mlit.go.jp/ksj/gml/datalist/KsjTmplt-N02-v2_3.html
http://nlftp.mlit.go.jp/ksj/gml/codelist/InstitutionTypeCd.html
http://nlftp.mlit.go.jp/ksj/gml/codelist/RailwayClassCd.html

## 2. Generate Data
### 2-1. Railway Data

```
ogr2ogr -f GeoJSON ./data/rail_road_1.geojson ./data/N02-18_RailroadSection.geojson -dialect sqlite -sql "SELECT N02_002 || '_' || RANK() OVER (ORDER BY N02_003, N02_004) as ID, N02_001, N02_002, N02_003, N02_004 FROM 'N02-18_RailroadSection' WHERE N02_002 = '1' GROUP BY N02_003, N02_004"

ogr2ogr -f GeoJSON ./data/rail_road_2.geojson ./data/N02-18_RailroadSection.geojson -dialect sqlite -sql "SELECT N02_002 || '_' || RANK() OVER (ORDER BY N02_003, N02_004) as ID, N02_001, N02_002, N02_003, N02_004 FROM 'N02-18_RailroadSection' WHERE N02_002 = '2' GROUP BY N02_003, N02_004"

ogr2ogr -f GeoJSON ./data/rail_road_3.geojson ./data/N02-18_RailroadSection.geojson -dialect sqlite -sql "SELECT N02_002 || '_' || RANK() OVER (ORDER BY N02_003, N02_004) as ID, N02_001, N02_002, N02_003, N02_004 FROM 'N02-18_RailroadSection' WHERE N02_002 = '3' GROUP BY N02_003, N02_004"

ogr2ogr -f GeoJSON ./data/rail_road_4.geojson ./data/N02-18_RailroadSection.geojson -dialect sqlite -sql "SELECT N02_002 || '_' || RANK() OVER (ORDER BY N02_003, N02_004) as ID, N02_001, N02_002, N02_003, N02_004 FROM 'N02-18_RailroadSection' WHERE N02_002 = '4' GROUP BY N02_003, N02_004"

ogr2ogr -f GeoJSON ./data/rail_road_5.geojson ./data/N02-18_RailroadSection.geojson -dialect sqlite -sql "SELECT N02_002 || '_' || RANK() OVER (ORDER BY N02_003, N02_004) as ID, N02_001, N02_002, N02_003, N02_004 FROM 'N02-18_RailroadSection' WHERE N02_002 = '5' GROUP BY N02_003, N02_004"
```

## 3. Put Records to Kinesis Data Stream
### 3-1. Prepare Railway Data for batch put-records
```
cat ./data/rail_road_1.geojson | jq -c '.features | {Records: map({Data: tojson, PartitionKey: (.properties.N02_004 + .properties.N02_003) }), StreamName: "amp_geojson" }'  > ./temp/rail_road_1.json

cat ./data/rail_road_2.geojson | jq -c '.features | {Records: map({Data: tojson, PartitionKey: (.properties.N02_004 + .properties.N02_003) }), StreamName: "amp_geojson" }'  > ./temp/rail_road_2.json

cat ./data/rail_road_3.geojson | jq -c '.features | {Records: map({Data: tojson, PartitionKey: (.properties.N02_004 + .properties.N02_003) }), StreamName: "amp_geojson" }'  > ./temp/rail_road_3.json

cat ./data/rail_road_4.geojson | jq -c '.features | {Records: map({Data: tojson, PartitionKey: (.properties.N02_004 + .properties.N02_003) }), StreamName: "amp_geojson" }'  > ./temp/rail_road_4.json

cat ./data/rail_road_5.geojson | jq -c '.features | {Records: map({Data: tojson, PartitionKey: (.properties.N02_004 + .properties.N02_003) }), StreamName: "amp_geojson" }'  > ./temp/rail_road_5.json
```

### 3-2. Batch put-records
#### Create the stream if not yet
```
aws kinesis create-stream --stream-name amp_geojson --shard-count 1
```

```
aws kinesis put-records --cli-input-json file://./temp/rail_road_1.json
aws kinesis put-records --cli-input-json file://./temp/rail_road_2.json
aws kinesis put-records --cli-input-json file://./temp/rail_road_3.json
aws kinesis put-records --cli-input-json file://./temp/rail_road_4.json
aws kinesis put-records --cli-input-json file://./temp/rail_road_5.json
```

## 4. Kinesis Data Analytics
### 4-1. SQL Sliding Window App
```
CREATE OR REPLACE STREAM "DESTINATION_SQL_STREAM" (
	RAILWAY_CLASS VARCHAR(10), 
	RAILWAY_CLASS_COUNT INTEGER, 
	EVENT_TIME TIMESTAMP
);

CREATE OR REPLACE PUMP "STREAM_PUMP" AS INSERT INTO "DESTINATION_SQL_STREAM"
	SELECT STREAM 
		CAST (N02_001 AS VARCHAR(10)) AS RAILWAY_CLASS, 
		COUNT(*) OVER (
			PARTITION BY N02_001
			RANGE INTERVAL '30' MINUTE PRECEDING
			) AS RAILWAY_CLASS_COUNT,
		ROWTIME AS EVENT_TIME
	FROM "SOURCE_SQL_STREAM_001";
```

#### Set the output to another Kinesis stream or firehose or Lambda to check the output.


### 4-2. SQL Tumbling Window App
```
CREATE OR REPLACE STREAM "DESTINATION_SQL_STREAM" (
	RAILWAY_CLASS VARCHAR(10), RAILWAY_CLASS_COUNT REAL
);

CREATE OR REPLACE PUMP "STREAM_PUMP" AS INSERT INTO "DESTINATION_SQL_STREAM"
    SELECT STREAM CAST (N02_001 AS VARCHAR(10)) AS RAILWAY_CLASS, COUNT(*)
        FROM "SOURCE_SQL_STREAM_001"
        GROUP BY N02_001, 
            STEP("SOURCE_SQL_STREAM_001".ROWTIME BY INTERVAL '60' SECOND);

```

#### Set the output to another Kinesis stream or firehose or Lambda to check the output.