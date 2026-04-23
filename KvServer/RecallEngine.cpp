#include "RecallEngine.h"
#include <chrono>
#include <functional>
#include <algorithm>

namespace featureServer {

int64_t RecallEngine::hashItemId(const std::string& itemId) const {
    static std::hash<std::string> hasher;
    return static_cast<int64_t>(hasher(itemId));
}

RecallEngine::RecallEngine(size_t maxElements,
                           size_t M,
                           size_t efConstruction,
                           size_t efSearch,
                           bool allowReplaceDeleted)
    : maxElements_(maxElements),
      M_(M),
      efConstruction_(efConstruction),
      efSearch_(efSearch) {
    m_space.reset(new hnswlib::InnerProductSpace(HNSW_DIM));
    m_index = new hnswlib::HierarchicalNSW<float>(
        m_space.get(),
        maxElements_,
        M_,
        efConstruction_,
        100,
        allowReplaceDeleted
    );
    m_index->ef_ = efSearch_;
}

RecallEngine::~RecallEngine() {
    delete m_index;
}

void RecallEngine::setEfSearch(size_t ef) {
    std::lock_guard<std::mutex> lk(m_mtx);
    if (m_index) {
        m_index->ef_ = ef;
    }
    efSearch_ = ef;
}

void RecallEngine::addPoint(const std::string& itemId,
                             const std::vector<float>& embedding) {
    int64_t label = hashItemId(itemId);
    {
        std::lock_guard<std::mutex> lk_label(m_labelMtx);
        if (m_activeLabels.count(label)) {
            return;
        }
    }
    std::lock_guard<std::mutex> lk(m_mtx);
    {
        std::lock_guard<std::mutex> lk_del(m_delMtx);
        m_deletedItems.erase(itemId);
    }
    m_labelToItemId[label] = itemId;
    {
        std::lock_guard<std::mutex> lk_label(m_labelMtx);
        m_activeLabels.insert(label);
    }
    m_index->addPoint(embedding.data(), label);
}

void RecallEngine::upsertPoint(const std::string& itemId,
                                const std::vector<float>& embedding) {
    int64_t label = hashItemId(itemId);
    bool existed;
    {
        std::lock_guard<std::mutex> lk_label(m_labelMtx);
        existed = m_activeLabels.count(label) != 0;
    }
    std::lock_guard<std::mutex> lk(m_mtx);
    {
        std::lock_guard<std::mutex> lk_del(m_delMtx);
        m_deletedItems.erase(itemId);
    }
    if (existed) {
        m_labelToItemId[label] = itemId;
        m_index->addPoint(embedding.data(), label, true);
    } else {
        {
            std::lock_guard<std::mutex> lk_label(m_labelMtx);
            m_activeLabels.insert(label);
        }
        m_labelToItemId[label] = itemId;
        m_index->addPoint(embedding.data(), label);
    }
}

void RecallEngine::deletePoint(const std::string& itemId) {
    int64_t label = hashItemId(itemId);
    bool existed;
    {
        std::lock_guard<std::mutex> lk_label(m_labelMtx);
        existed = m_activeLabels.count(label) != 0;
    }
    if (!existed) return;

    std::lock_guard<std::mutex> lk(m_mtx);
    m_index->markDelete(label);
    {
        std::lock_guard<std::mutex> lk_del(m_delMtx);
        m_deletedItems.insert(itemId);
    }
    {
        std::lock_guard<std::mutex> lk_label(m_labelMtx);
        m_activeLabels.erase(label);
    }
}

RecallEngine::SearchResult
RecallEngine::searchTopK(const std::vector<float>& query,
                          int topK,
                          int64_t* searchTimeUs) const {
    SearchResult result{{}, {}};

    auto start = std::chrono::high_resolution_clock::now();

    std::vector<std::pair<float, size_t>> knn;
    {
        std::lock_guard<std::mutex> lk(m_mtx);
        if (!m_index || m_index->cur_element_count == 0) {
            auto end = std::chrono::high_resolution_clock::now();
            *searchTimeUs = std::chrono::duration_cast<std::chrono::microseconds>(
                                end - start).count();
            return result;
        }
        auto res = m_index->searchKnnCloserFirst(query.data(), topK);
        for (auto& p : res) {
            knn.emplace_back(p.first, static_cast<size_t>(p.second));
        }
    }

    result.first.reserve(knn.size());
    result.second.reserve(knn.size());

    std::unordered_set<std::string> delSet;
    {
        std::lock_guard<std::mutex> lk_del(m_delMtx);
        delSet = m_deletedItems;
    }

    for (auto& p : knn) {
        auto it = m_labelToItemId.find(static_cast<int64_t>(p.second));
        if (it != m_labelToItemId.end()) {
            if (delSet.find(it->second) != delSet.end()) {
                continue;
            }
            result.first.push_back(it->second);
            result.second.push_back(p.first);
        }
    }

    auto end = std::chrono::high_resolution_clock::now();
    *searchTimeUs = std::chrono::duration_cast<std::chrono::microseconds>(
                        end - start).count();
    return result;
}

size_t RecallEngine::size() const {
    std::lock_guard<std::mutex> lk(m_mtx);
    return m_index ? m_index->cur_element_count.load() : 0;
}

size_t RecallEngine::deletedCount() const {
    std::lock_guard<std::mutex> lk_del(m_delMtx);
    return m_deletedItems.size();
}

// ==================== 持久化 ====================

std::string RecallEngine::getIndexPath(const std::string& persistDir, int me) {
    return persistDir + "/hnsw_index_" + std::to_string(me) + ".bin";
}

void RecallEngine::saveIndex(const std::string& path) {
    if (!m_index) return;
    std::lock_guard<std::mutex> lk(m_mtx);
    m_index->saveIndex(path);

    std::string mapPath = path + ".labels";
    std::ofstream ofs(mapPath, std::ios::binary);
    if (!ofs.is_open()) {
        std::cerr << "[RecallEngine] Failed to open label map file for write: "
                  << mapPath << std::endl;
        return;
    }
    int32_t count = static_cast<int32_t>(m_labelToItemId.size());
    ofs.write(reinterpret_cast<const char*>(&count), sizeof(count));
    for (const auto& kv : m_labelToItemId) {
        ofs.write(reinterpret_cast<const char*>(&kv.first), sizeof(kv.first));
        int32_t strLen = static_cast<int32_t>(kv.second.size());
        ofs.write(reinterpret_cast<const char*>(&strLen), sizeof(strLen));
        ofs.write(kv.second.data(), strLen);
    }
    int32_t delCount = static_cast<int32_t>(m_deletedItems.size());
    ofs.write(reinterpret_cast<const char*>(&delCount), sizeof(delCount));
    for (const auto& itemId : m_deletedItems) {
        int32_t strLen = static_cast<int32_t>(itemId.size());
        ofs.write(reinterpret_cast<const char*>(&strLen), sizeof(strLen));
        ofs.write(itemId.data(), strLen);
    }
    ofs.close();
    std::cout << "[RecallEngine] Index saved: " << path
              << ", labels=" << count
              << ", deleted=" << delCount << std::endl;
}

int RecallEngine::loadIndex(const std::string& path) {
    std::ifstream ifs(path, std::ios::binary);
    if (!ifs.is_open()) {
        return -1;
    }
    ifs.close();

    std::lock_guard<std::mutex> lk(m_mtx);

    delete m_index;
    m_space.reset(new hnswlib::InnerProductSpace(HNSW_DIM));
    m_index = new hnswlib::HierarchicalNSW<float>(m_space.get(), path);
    m_labelToItemId.clear();
    {
        std::lock_guard<std::mutex> lk_label(m_labelMtx);
        m_activeLabels.clear();
    }

    std::string mapPath = path + ".labels";
    std::ifstream lfs(mapPath, std::ios::binary);
    if (lfs.is_open()) {
        int32_t count = 0;
        lfs.read(reinterpret_cast<char*>(&count), sizeof(count));
        for (int32_t i = 0; i < count; ++i) {
            int64_t label = 0;
            lfs.read(reinterpret_cast<char*>(&label), sizeof(label));
            int32_t strLen = 0;
            lfs.read(reinterpret_cast<char*>(&strLen), sizeof(strLen));
            std::string itemId(strLen, '\0');
            lfs.read(&itemId[0], strLen);
            m_labelToItemId[label] = itemId;
            {
                std::lock_guard<std::mutex> lk_label(m_labelMtx);
                m_activeLabels.insert(label);
            }
        }
        int32_t delCount = 0;
        lfs.read(reinterpret_cast<char*>(&delCount), sizeof(delCount));
        std::lock_guard<std::mutex> lk_del(m_delMtx);
        for (int32_t i = 0; i < delCount; ++i) {
            int32_t strLen = 0;
            lfs.read(reinterpret_cast<char*>(&strLen), sizeof(strLen));
            std::string itemId(strLen, '\0');
            lfs.read(&itemId[0], strLen);
            m_deletedItems.insert(itemId);
        }
        lfs.close();
        std::cout << "[RecallEngine] Index loaded: " << path
                  << ", labels=" << count
                  << ", deleted=" << delCount << std::endl;
        return count;
    }

    std::cerr << "[RecallEngine] Label map file not found: " << mapPath
              << ", rebuilding from labels..." << std::endl;
    return -1;
}

}  // namespace featureServer
