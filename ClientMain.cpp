#include "myRPC/Server/Krpcapplication.h"
#include "Clerk/clerk.h"

#include <iostream>
#include <vector>
struct BenchResult{
    std::vector<long long> latenciesUs;//每次请求耗时微妙
}

//run_benchmark函数框架
void run_benchmark(const std::string& configPath,
                    int totalOps,
                    int threads,
                    int writeRatio,
                    int keySpace,
                    int valueSize){
    using Clock=std::chrono::steady_clock;
    std::vector<std::thread> workers;
    std::vector<BenchResult> results(threads);
    int opsPerThread=totalOps/threads;
    auto startAll =Clock::now();
    for(int i=0;i<threads;i++){
        workers.emplace_back([&,i](){
            //每个线程自己创建clerk 更简单 不用考虑线程安全
            Clerk clerk(configPath);
            BenchResult &r=results[i];
            r.latenciesUs.reserve(opsPerThread);
            std::mt19937 gen((unsigned)Clock::now().time_since_epoch().count()+i);
            std::uniform_int_distrubution<int> opDist(0,99);
            for(int j=0;j<opsPerThread;j++){
                std::string key=random_key(keySpace,gen);
                bool doWrite=opDist(gen) < writeRatio;
                auto t1=Clock::now();
                if(doWrite){
                    std::string val=random_value(valueSize,gen);
                    clerk.Put(key,val);
                }else{
                    std::string val=clerk.Get(key);
                }
                auto t2=Clock::now();
                 long long us = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count();
                r.latenciesUs.push_back(us);
        });
    }      
    for(auto &th:workers)   th.join();
    auto endAll=Clock::now();  
    
    //统计并输出
    auto totalUs=std::chrono::duration_cast<std::chrono::microseconds>(endAll-startAll);
    long long totalOpsReal=0;
    std::vector<long long> allLatencies;
    for(int t=0;t<threads;++t){
        totalOpsReal+=results[t].latenciesUs.size();
        allLatencies.insert(allLatencies.end(),
                            results[t].latenciesUs.begin(),
                            results[t].latenciesUs.end());
    }
    double totalSec=totalUs/1e6;
    double qps=totalOpsReal/totalSec;

    //计算平均和
    std::sort(allLatencies.begin(),allLatencies.end());
    double avgUs=std::accumulate(allLatencies.begin(),allLatencies.end(),0.0)/allLatencies.size();
    auto p95=allLatencies[(size_t)(allLatencies.size()*0.95)];
    auto p99=allLatencies[(size_t)(allLatenciess.size()*0.99)];
    std::cout << "总请求数: " << totalOpsReal << "\n";
    std::cout << "总耗时: " << totalSec << " s\n";
    std::cout << "QPS: " << qps << " ops/s\n";
    std::cout << "平均延迟: " << avgUs / 1000.0 << " ms\n";
    std::cout << "p95 延迟: " << p95 / 1000.0 << " ms\n";
    std::cout << "p99 延迟: " << p99 / 1000.0 << " ms\n";
}
int main(int argc,char** argv){
    //新增一组变量用来支持--bench 模式
    bool benchMode=false;
    int totalOps=10000;
    int threads=4;
    int writeRatio=50;
    int keySpace=1000;
    int valueSize=128;


    //1.初始化myRPC框架，读取conf/myrpc.conf  这里只是读取-i参数 
    KrpcApplication::Init(argc,argv);
    //在后面添加参数解析
    bool startParsing=false;
    for(int i=1;i<argv.size();i++){
        std::string arg=argv[i];
        //1.定位分隔符""--"
        if(arg=="--"){
            startParsing=true;
            continue;
        }
        if(!startParsing)   continue;
        //2.解析逻辑
        if(arg=="--bench"){
            benchMode=true;
        }else if(arg=="--ops" && i+1<argc){
            toatlOps=std::atoi(argv[++i]);
        }else if(arg=="--threads" && i+1< argc){
            threads=std::atoi(argv[++i]);
        }else if(arg=="--write-ratio" && i+1 <argc){
            writeRatio=std::atoi(argv[++i]);
        }else if(arg=="--key-space" && i+1 <argc){
            keySpace=std::atoi(argv[++i]);
        }else if(arg=="--value-size" && i+1 <argc){
            valueSize=std::atoi(argv[++i]);
        }
    }
    //根据是否是压测模式 分流
    if(!benchMode){
        //3.做几次简单的Put、GET测试
        //2.初始化Clerk （生成ClientId/RequestID）
        Clerk ck;
        ck.Init();
        //普通模式
        ck.Put("foo","hello");
        ck.Put("bar","world");
        std::string v1=ck.Get("foo");
        std::string v2=ck.Get("bar");
        std::cout << "Get(foo)= "<<v1<< std::endl;
        std::cout <<"Get(bar)= " << v2 <<std::endl;
    }else{
        //压测模式：创建多个线程每个线程循环Clerk::put/Get，记录耗时 最后统计
        run_benchmark(configPath,totalOps,threads,writeRatio,keySpace,valueSize);
    }
    
    return 0;
}