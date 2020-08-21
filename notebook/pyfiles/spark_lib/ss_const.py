# 除外対象UUID
EXCLUDE_UUID = [
         'idfa:',
         'anid:',
         'imei:',
         'idfa:00000000-0000-0000-0000-000000000000',
         'anid:00000000-0000-0000-0000-000000000000',
         'imei:0',
         'imei:00000000',
         'imei:000000000000000',
         'YYYYYYYYYYYYYYYYYYYYYYYY',
         'XXXXXXXXXXXXXXXXXXXXXXXX' ]

# 推測等で利用する主要ssp_page_id
TARGET_SSP_PAGE_ID = {
    10392: [10392], # adgen + appvador
     8085: [8085],  # google
    12372: [12372], # PubMatic
    32603: [32603], # BidSwitch
    16535: [16535], # Rubicon
    24301: [24301], # AOL
     8086: [8086],  # OpenX
}
