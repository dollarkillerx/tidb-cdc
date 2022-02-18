# 创建任务， 监听 raw 库中的 snapshots 表的数据变更， 发送到 snapshots topic
`tiup cdc:v5.2.2 cli changefeed create --pd=http://192.168.88.203:2379 --sink-uri="kafka://192.168.88.203:9092/deal_test.deals?kafka-version=2.7.1&partition-num=3&max-message-bytes=67108864&replication-factor=1" --config deals_changefeed.toml -c deal-test-deals`

# 查看所有任务状态
`tiup cdc:v5.2.2 cli changefeed list --pd=http://192.168.88.203:2379`

# 创建任务时会如果指定 Topic 不存在会创建， 如果上面没报错就手动验证能不能捕获到数据变更
