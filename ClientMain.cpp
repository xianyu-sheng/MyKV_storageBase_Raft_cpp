#include "myRPC/Server/Krpcapplication.h"
#include "Clerk/clerk.h"
#include "../Proto/raftKVRpcProtoc/KvServerRPC.pb.h"

#include <iostream>
#include <vector>
#include <thread>
#include <chrono>
#include <algorithm>
#include <numeric>
#include <cstdlib>
#include <random>
#include <glog/logging.h>
#include <sys/stat.h>

struct BenchResult {
    std::vector<long long> latenciesUs;
};

bool benchMode = false;
int totalOps = 200;
int threads = 4;
int writeRatio = 50;
int keySpace = 1000;
int valueSize = 128;

// ========== Search 压测参数 ==========
bool searchBenchMode = false;
int searchTopK = 10;
int warmupItems = 100;   // 预热时写入多少条商品特征
int warmupThreads = 4;

void run_search_benchmark(int totalOps, int threads, int topK) {
    using Clock = std::chrono::steady_clock;
    std::vector<std::thread> workers;
    std::vector<BenchResult> results(threads);
    int opsPerThread = totalOps / threads;

    std::cout << "[SearchBench] ops=" << totalOps
              << " threads=" << threads
              << " topK=" << topK << std::endl;

    auto startAll = Clock::now();
    for (int i = 0; i < threads; ++i) {
        workers.emplace_back([&, i]() {
            Clerk clerk;
            clerk.Init();
            BenchResult& r = results[i];
            r.latenciesUs.reserve(opsPerThread);
            std::mt19937 gen((unsigned)Clock::now().time_since_epoch().count() + i);
            std::uniform_int_distribution<int> keyDist(0, warmupItems - 1);

            for (int j = 0; j < opsPerThread; ++j) {
                // 随机生成 128 维查询向量
                std::vector<float> query;
                query.reserve(128);
                for (int d = 0; d < 128; ++d) {
                    std::uniform_real_distribution<float> fdist(0.0f, 1.0f);
                    query.push_back(fdist(gen));
                }

                auto t1 = Clock::now();
                auto res = clerk.Search(query, topK);
                auto t2 = Clock::now();

                long long us = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count();
                r.latenciesUs.push_back(us);
            }
        });
    }

    for (auto& th : workers) th.join();
    auto endAll = Clock::now();

    auto totalUs = std::chrono::duration_cast<std::chrono::microseconds>(endAll - startAll);
    long long totalOpsReal = 0;
    std::vector<long long> allLatencies;
    for (int t = 0; t < threads; ++t) {
        totalOpsReal += results[t].latenciesUs.size();
        allLatencies.insert(allLatencies.end(),
                            results[t].latenciesUs.begin(),
                            results[t].latenciesUs.end());
    }
    double totalSec = totalUs.count() / 1e6;
    double qps = totalOpsReal / totalSec;

    std::sort(allLatencies.begin(), allLatencies.end());
    double avgUs = std::accumulate(allLatencies.begin(), allLatencies.end(), 0.0) / allLatencies.size();
    auto p50 = allLatencies[(size_t)(allLatencies.size() * 0.50)];
    auto p95 = allLatencies[(size_t)(allLatencies.size() * 0.95)];
    auto p99 = allLatencies[(size_t)(allLatencies.size() * 0.99)];
    auto minUs = allLatencies.front();
    auto maxUs = allLatencies.back();

    std::cout << "\n========== Search Benchmark Results ==========\n";
    std::cout << "总请求数: " << totalOpsReal << "\n";
    std::cout << "总耗时: " << totalSec << " s\n";
    std::cout << "QPS: " << qps << " ops/s\n";
    std::cout << "平均延迟: " << avgUs / 1000.0 << " ms\n";
    std::cout << "P50 延迟: " << p50 / 1000.0 << " ms\n";
    std::cout << "P95 延迟: " << p95 / 1000.0 << " ms\n";
    std::cout << "P99 延迟: " << p99 / 1000.0 << " ms\n";
    std::cout << "Min 延迟: " << minUs / 1000.0 << " ms\n";
    std::cout << "Max 延迟: " << maxUs / 1000.0 << " ms\n";
    std::cout << "===========================================\n";
}

void warmup_features(int numItems, int numThreads) {
    std::cout << "[Warmup] Inserting " << numItems << " item features..." << std::endl;
    std::vector<std::thread> workers;
    int itemsPerThread = numItems / numThreads;

    for (int t = 0; t < numThreads; ++t) {
        workers.emplace_back([=]() {
            Clerk clerk;
            clerk.Init();
            std::mt19937 gen((unsigned)clock() + t);
            std::uniform_real_distribution<float> fdist(0.0f, 1.0f);

            int startId = t * itemsPerThread;
            int endId = (t == numThreads - 1) ? numItems : startId + itemsPerThread;

            for (int i = startId; i < endId; ++i) {
                raftKVRpcProtoc::ItemFeature feat;
                feat.set_item_id("item_" + std::to_string(i));
                for (int d = 0; d < 128; ++d) {
                    feat.add_embedding(fdist(gen));
                }
                feat.set_timestamp(static_cast<int64_t>(clock()));
                clerk.PutFeature(feat);
            }
        });
    }

    for (auto& th : workers) th.join();
    std::cout << "[Warmup] Done." << std::endl;
}

void run_kv_benchmark(int totalOps, int threads,
                      int writeRatio, int keySpace, int valueSize) {
    using Clock = std::chrono::steady_clock;
    std::vector<std::thread> workers;
    std::vector<BenchResult> results(threads);
    int opsPerThread = totalOps / threads;

    auto startAll = Clock::now();
    for (int i = 0; i < threads; ++i) {
        workers.emplace_back([&, i]() {
            Clerk clerk;
            clerk.Init();
            clerk.Put("__bench_warmup__", "1");
            BenchResult& r = results[i];
            r.latenciesUs.reserve(opsPerThread);
            std::mt19937 gen((unsigned)Clock::now().time_since_epoch().count() + i);
            std::uniform_int_distribution<int> opDist(0, 99);

            for (int j = 0; j < opsPerThread; ++j) {
                std::string key = random_key(keySpace, gen);
                bool doWrite = opDist(gen) < writeRatio;
                auto t1 = Clock::now();
                if (doWrite) {
                    std::string val = random_value(valueSize, gen);
                    clerk.Put(key, val);
                } else {
                    clerk.Get(key);
                }
                auto t2 = Clock::now();
                long long us = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count();
                r.latenciesUs.push_back(us);
            }
        });
    }

    for (auto& th : workers) th.join();
    auto endAll = Clock::now();

    auto totalUs = std::chrono::duration_cast<std::chrono::microseconds>(endAll - startAll);
    long long totalOpsReal = 0;
    std::vector<long long> allLatencies;
    for (int t = 0; t < threads; ++t) {
        totalOpsReal += results[t].latenciesUs.size();
        allLatencies.insert(allLatencies.end(),
                            results[t].latenciesUs.begin(),
                            results[t].latenciesUs.end());
    }
    double totalSec = totalUs.count() / 1e6;
    double qps = totalOpsReal / totalSec;

    std::sort(allLatencies.begin(), allLatencies.end());
    double avgUs = std::accumulate(allLatencies.begin(), allLatencies.end(), 0.0) / allLatencies.size();
    auto p95 = allLatencies[(size_t)(allLatencies.size() * 0.95)];
    auto p99 = allLatencies[(size_t)(allLatencies.size() * 0.99)];

    std::cout << "\n========== KV Benchmark Results ==========\n";
    std::cout << "总请求数: " << totalOpsReal << "\n";
    std::cout << "总耗时: " << totalSec << " s\n";
    std::cout << "QPS: " << qps << " ops/s\n";
    std::cout << "平均延迟: " << avgUs / 1000.0 << " ms\n";
    std::cout << "P95 延迟: " << p95 / 1000.0 << " ms\n";
    std::cout << "P99 延迟: " << p99 / 1000.0 << " ms\n";
    std::cout << "========================================\n";
}

int main(int argc, char** argv) {
    mkdir("log", 0777);
    google::InitGoogleLogging(argv[0]);

    for (int i = 1; i < argc; ++i) {
        if (std::string(argv[i]) == "--bench") {
            benchMode = true;
            break;
        }
        if (std::string(argv[i]) == "--search-bench") {
            searchBenchMode = true;
            benchMode = true;
            break;
        }
    }

    FLAGS_minloglevel = 2;
    FLAGS_logtostderr = false;
    FLAGS_alsologtostderr = false;
    FLAGS_log_dir = "./log";
    FLAGS_max_log_size = 100;

    KrpcApplication::Init(argc, argv);

    bool startParsing = false;
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--") {
            startParsing = true;
            continue;
        }
        if (!startParsing) continue;

        if (arg == "--bench") {
            benchMode = true;
        } else if (arg == "--search-bench") {
            searchBenchMode = true;
            benchMode = true;
        } else if (arg == "--ops" && i + 1 < argc) {
            totalOps = std::atoi(argv[++i]);
        } else if (arg == "--threads" && i + 1 < argc) {
            threads = std::atoi(argv[++i]);
        } else if (arg == "--write-ratio" && i + 1 < argc) {
            writeRatio = std::atoi(argv[++i]);
        } else if (arg == "--key-space" && i + 1 < argc) {
            keySpace = std::atoi(argv[++i]);
        } else if (arg == "--value-size" && i + 1 < argc) {
            valueSize = std::atoi(argv[++i]);
        } else if (arg == "--search-topk" && i + 1 < argc) {
            searchTopK = std::atoi(argv[++i]);
        } else if (arg == "--warmup-items" && i + 1 < argc) {
            warmupItems = std::atoi(argv[++i]);
        }
    }

    if (!benchMode) {
        Clerk ck;
        ck.Init();
        ck.Put("foo", "hello");
        ck.Put("bar", "world");
        std::string v1 = ck.Get("foo");
        std::string v2 = ck.Get("bar");
        std::cout << "Get(foo)= " << v1 << std::endl;
        std::cout << "Get(bar)= " << v2 << std::endl;
    } else if (searchBenchMode) {
        warmup_features(warmupItems, warmupThreads);
        std::this_thread::sleep_for(std::chrono::seconds(2));
        run_search_benchmark(totalOps, threads, searchTopK);
    } else {
        run_kv_benchmark(totalOps, threads, writeRatio, keySpace, valueSize);
    }

    return 0;
}
