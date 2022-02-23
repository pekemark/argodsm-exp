#ifndef argo_statistics_hpp
#define argo_statistics_hpp argo_statistics_hpp

#include <atomic>

/** @brief Struct containing statistics */
struct argo_statistics
{
	/** @brief Time spend locking */
	std::atomic<double> locktime;
	/** @brief Time spent self invalidating */
	std::atomic<double> selfinvtime; 
	/** @brief Time spent loading pages */
	std::atomic<double> loadtime;
	/** @brief Time spent storing pages */
	std::atomic<double> storetime; 
	/** @brief Time spent writing back from the writebuffer */
	std::atomic<double> writebacktime; 
	/** @brief Time spent flushing the writebuffer */
	std::atomic<double> flushtime; 
	/** @brief Time spent in global barrier */
	std::atomic<double> barriertime; 
	/** @brief Number of stores (i.e., entire cache blocks written back for any reason) */
	std::atomic<unsigned long> stores; 
	/** @brief Number of loads (i.e., cache blocks loaded) */
	std::atomic<unsigned long> loads; 
	/** @brief Number of barriers executed */
	std::atomic<unsigned long> barriers; 
	/** @brief Number of writebacks from (full) writebuffer */
	std::atomic<unsigned long> writebacks; 
	/** @brief Number of locks taken */
	std::atomic<unsigned long> locks;
	/** @brief Number of locks released */
	std::atomic<unsigned long> unlocks;
	/** @brief Number of locks transferred (taken from another node) */
	std::atomic<unsigned long> locktransfers;
	/** @brief Time spent performing selective acquire */
	std::atomic<double> ssitime;
	/** @brief Time spent performing selective release */
	std::atomic<double> ssdtime;

	/** @brief Resets all variables to zero. */
	void clear();

	/** @brief Prints the collected statistics.
	 * @param node integer to use as an identifier for the ArgoDSM node producing the statistics.
	 */
	void print(int node);

};

extern argo_statistics stats;

#endif /* argo_statistics_hpp */