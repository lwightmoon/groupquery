# 环境安装
1. 下载
https://golang.org/doc/install/source?download=go1.15.6.src.tar.gz
2. 解压到指定目录dir
3. export GOROOT=dir/go

# 运行
mkdir -p ~/workspace/src
cd ~/workspace/src
export GOPATH=$GOPATH:~/workspace
cd ~/workspace/src
git clone 
go build -o groupquery
./groupquery -f data.csv

# 测试
go test -v  ./...

# 设计思路
使用mapreduce的处理思路
1. 按照 group key进行shuffle得到分区后的数据
2. 对各数据分区进行并发排序
3. 对排序后的数据中属于相同 group key 的数据执行聚合函数
4. 输出结果到控制台

# 存在的问题
## 内存不足的情况
1. 生成f(b,[a1,a2,a3...an])的中间表
2. 分批读取原始table的数据，写入到中间表中
3. 读取中间表得到b对应的数组执行聚合函数

# 单机处理性能问题

1. 将原始数据x分成m个文件如[x1.csv,x2.csv...xm.csv]
2. master分发m个文件到多台node执行读取操作同时按照b字段进行分片生成n个中间文件[x1_1.csv,x1_2.csv...x1_n.csv];将中间文件写入类似hdfs的分布式文件系统,总共生成m*n个文件
3. 等待分片完成之后，master根据任务编号[1,n]分发任务到woker实例，例如一个实例获取到编号为1的任务则拉取[x1_1.csv,x2_1.csv...xm_1.csv]的文件；执行多路归并排序
4. 每个worker对排序好的文件执行遍历；对b的取值有相同value的记录执行聚合函数,将结果写入ret[y].csv文件 y为上一步中的任务编号
5. 合并多个worker实例产生的文件