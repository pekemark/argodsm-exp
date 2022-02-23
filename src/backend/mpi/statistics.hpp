#ifndef argo_statistics_hpp
#define argo_statistics_hpp argo_statistics_hpp

#include <atomic>

/** @brief Struct containing statistics */
struct argo_statistics
{
	/** @brief Time spend locking */
	double locktime;
	/** @brief Time spent self invalidating */
	double selfinvtime; 
	/** @brief Time spent loading pages */
	double loadtime;
	/** @brief Time spent storing pages */
	double storetime; 
	/** @brief Time spent writing back from the writebuffer */
	double writebacktime; 
	/** @brief Time spent flushing the writebuffer */
	double flushtime; 
	/** @brief Time spent in global barrier */
	double barriertime; 
	/** @brief Number of stores (i.e., entire cache blocks written back for any reason) */
	unsigned long stores; 
	/** @brief Number of loads (i.e., cache blocks loaded) */
	unsigned long loads; 
	/** @brief Number of barriers executed */
	unsigned long barriers; 
	/** @brief Number of writebacks from (full) writebuffer */
	unsigned long writebacks; 
	/** @brief Number of locks taken */
	unsigned long locks;
	/** @brief Number of locks released */
	unsigned long unlocks;
	/** @brief Number of locks transferred (taken from another node) */
	unsigned long locktransfers;
	/** @brief Time spent performing selective acquire */
	double ssitime;
	/** @brief Time spent performing selective release */
	double ssdtime;

	/** @brief Resets all variables to zero. */
	void clear();

	/** @brief Prints the collected statistics.
	 * @param node integer to use as an identifier for the ArgoDSM node producing the statistics.
	 */
	void print(int node);

};

extern argo_statistics stats;

#endif /* argo_statistics_hpp */