#include "myRPC/Server/Krpcapplication.h"
#include "Clerk/clerk.h"

#include <iostream>

int main(int argc,char** argv){
    //1.初始化myRPC框架，读取conf/myrpc.conf
    KrpcApplication::Init(argc,argv);
    //2.初始化Clerk （生成ClientId/RequestID）
    Clerk ck;
    ck.Init();
    //3.做几次简单的Put、GET测试
    ck.Put("foo","hello");
    ck.Put("bar","world");
    std::string v1=ck.Get("foo");
    std::string v2=ck.Get("bar");
    std::cout << "Get(foo)= "<<v1<< std::endl;
    std::cout <<"Get(bar)= " << v2 <<std::endl;
    return 0;
}