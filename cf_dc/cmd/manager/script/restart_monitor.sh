#!/bin/bash

# 检查输入参数
if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <restart|sync>"
  exit 1
fi

action=$1

# 检查参数是否为 restart 或 sync
if [[ "$action" != "restart" && "$action" != "sync" ]]; then
  echo "Invalid argument: $action"
  echo "Usage: $0 <restart|sync>"
  exit 1
fi

target_directories=(
    "/data/vhosts/cf_dc/monitor_dcpro1"
    "/data/vhosts/cf_dc/monitor_dcpro2"
    "/data/vhosts/cf_dc/monitor_dcpro3"
    "/data/vhosts/cf_dc/monitor_dcpro4"
    "/data/vhosts/cf_dc/monitor_dcpro5"
    "/data/vhosts/cf_dc/monitor_dcpro6"
    "/data/vhosts/cf_dc/monitor_dcpro7"
    "/data/vhosts/cf_dc/monitor_dcpro8"
    "/data/vhosts/cf_dc/monitor_dcpro9"
    "/data/vhosts/cf_dc/monitor_dcpro10"
    "/data/vhosts/cf_dc/monitor_dcpro11"
    "/data/vhosts/cf_dc/monitor_dcpro12"
    "/data/vhosts/cf_dc/monitor_dcob1"
)

# 移动文件到目标目录并创建软链接
for dir in "${target_directories[@]}"; do
  # 进入目录
  cd "$dir" || continue

  echo "Executing './cf.py $action' in $dir"
  chmod +x ./cf.py
  ./cf.py "$action"
  echo "Execution result: $?"

  # 回到脚本所在目录
  cd - > /dev/null
done
