#pragma once
#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <mutex>
#include <memory>
#include <fstream>
#include <sstream>
#include "hnswlib.h"
#include "../Proto/raftKVRpcProtoc/KvServerRPC.pb.h"

namespace featureServer {

constexpr size_t HNSW_DIM = 128;
constexpr size_t HNSW_MAX_ELEMENTS = 1'000'000;
constexpr size_t HNSW_M = 16;
constexpr size_t HNSW_EFC = 200;
constexpr size_t HNSW_EF_SEARCH = 50;

class RecallEngine {
public:
    using SearchResult = std::pair<std::vector<std::string>, std::vector<float>>;

    explicit RecallEngine(size_t maxElements = HNSW_MAX_ELEMENTS,
                          size_t M = HNSW_M,
                          size_t efConstruction = HNSW_EFC,
                          size_t efSearch = HNSW_EF_SEARCH,
                          bool allowReplaceDeleted = true);

    ~RecallEngine();

    void setEfSearch(size_t ef);

    void addPoint(const std::string& itemId,
                  const std::vector<float>& embedding);

    // Update: replaces existing point's embedding if itemId already exists
    void upsertPoint(const std::string& itemId,
                     const std::vector<float>& embedding);

    // Soft delete: mark itemId as deleted (filtered out from search results)
    void deletePoint(const std::string& itemId);

    SearchResult searchTopK(const std::vector<float>& query, int topK,
                            int64_t* searchTimeUs) const;

    size_t size() const;

    // 删除计数（用于监控）
    size_t deletedCount() const;

    size_t maxElements() const { return maxElements_; }

    bool isReady() const { return m_index != nullptr; }

    // 持久化：保存 HNSW 图到磁盘文件
    void saveIndex(const std::string& path);

    // 持久化：从磁盘加载 HNSW 图 + 恢复 label 映射
    // 返回实际加载的向量数量，-1 表示文件不存在（走全量构建）
    int loadIndex(const std::string& path);

    // 获取持久化文件路径（供调用方构造路径用）
    static std::string getIndexPath(const std::string& persistDir, int me);

    template<typename SkipListKv>
    void buildFromSkipList(const SkipListKv& kvdb);

private:
    int64_t hashItemId(const std::string& itemId) const;

private:
    hnswlib::HierarchicalNSW<float>* m_index = nullptr;
    std::unique_ptr<hnswlib::SpaceInterface<float>> m_space;

    std::unordered_map<int64_t, std::string> m_labelToItemId;
    mutable std::mutex m_mtx;

    // 软删除集合：已删除但仍保留在 HNSW 图中的 itemId（用于搜索过滤）
    std::unordered_set<std::string> m_deletedItems;
    mutable std::mutex m_delMtx;

    // 当前 m_labelToItemId 中的 label 集合（用于判断是否存在，用于 upsert）
    std::unordered_set<int64_t> m_activeLabels;
    mutable std::mutex m_labelMtx;

    const size_t maxElements_;
    const size_t M_;
    const size_t efConstruction_;
    size_t efSearch_;
};

template<typename SkipListKv>
void RecallEngine::buildFromSkipList(const SkipListKv& kvdb) {
    kvdb.foreach ([this](const std::string& itemId,
                         const std::string& value) {
        raftKVRpcProtoc::ItemFeature feat;
        if (feat.ParseFromString(value)) {
            if (feat.embedding_size() == HNSW_DIM) {
                std::vector<float> emb;
                emb.reserve(HNSW_DIM);
                for (int i = 0; i < HNSW_DIM; ++i) {
                    emb.push_back(feat.embedding(i));
                }
                addPoint(itemId, emb);
            }
        }
    });
}

}  // namespace featureServer
