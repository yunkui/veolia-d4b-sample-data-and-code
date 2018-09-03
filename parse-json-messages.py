# -*- coding: utf-8 -*-
import logging  
import json
import time
import sys
import traceback
import random

from datahub import DataHub
from datahub.exceptions import ResourceExistException
from datahub.models import FieldType, RecordSchema, TupleRecord, BlobRecord, CursorType, RecordType

def handler(event, context):
  logger = logging.getLogger()

  evt = json.loads(event)
  #print("[print1]IoT trigger and send data to FunctionCompute test output, The content of event is : %s" % (evt))
  
  timestamp = evt['timestamp']
  values = evt['values']
  count_of_value = len(values)

  ACCESS_ID = 'XXXXX'
  ACCESS_KEY = 'XXXXX'
  ENDPOINT = 'http://dh-cn-XXXXX.aliyun-inc.com'
  dh = DataHub(ACCESS_ID, ACCESS_KEY, ENDPOINT)
  	
  PROJECT_NAME = 'veolia_d4b_poc'
  TOPIC_NAME = 'extract_result_table'
  	
  # ===================== put tuple records =====================
  # block等待所有shard状态ready
  dh.wait_shards_ready(PROJECT_NAME, TOPIC_NAME)

  topic = dh.get_topic(PROJECT_NAME, TOPIC_NAME)
  record_schema = topic.record_schema

  shards_result = dh.list_shard(PROJECT_NAME, TOPIC_NAME)
  shards = shards_result.shards
  shard_count = len(shards)
#   for shard in shards:
#     print("[print8]IoT trigger and send data to FunctionCompute test output, The Shard is : (%s)" % (shard))

  records = []
  
  for value in values:
    # id sample: SE433_OPC.S01.AISA0101
    id = value['id']
    id_list = id.split('.')
    id_company_code  = (id_list[0].split('_'))[0]
    id_protocol_name = (id_list[0].split('_'))[1]
    id_system_code   =  id_list[1]
    id_tagname       =  id_list[2]
    
    v = value['v']
    q = 'true' if value['q'] else 'false'
    t = value['t']
    #print("[print7]IoT trigger and send data to FunctionCompute test output, The value is : (%s, %s, %s, %s)" % (id,v,q,t)) 
    
    rec = TupleRecord(schema=topic.record_schema)
    rec.values = [timestamp, id_company_code, id_protocol_name, id_system_code, id_tagname, v, q, t]
    rec.shard_id = shards[random.randint(0,shard_count-1)].shard_id
    records.append(rec)
    
  failed_indexs = dh.put_records(PROJECT_NAME, TOPIC_NAME, records)
  print("[print9] put tuple %d records, shard_id = %s, failed list: %s" % (len(records), rec.shard_id, failed_indexs))
  # failed_indexs如果非空最好对failed record再进行重试

  return 'success'
  
# event样例：
# {
# 	"timestamp":1521698375065,
# 	"values":[
# 		{
# 			"id":"SE433_OPC.S01.IW1440",
# 			"v":206,
# 			"q":true,
# 			"t":1521698358299
# 		},
# 		{
# 			"id":"SESE433_OPC433.S01.LCV1414_ACT",
# 			"v":42,
# 			"q":true,
# 			"t":1521698358222
# 		},
# 		{
# 			"id":"SE433_OPC.S01.LT1430A",
# 			"v":22,
# 			"q":true,
# 			"t":1521698358235
# 		},
# 		… 	
# 	]
# }