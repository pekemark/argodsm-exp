#ifndef argo_persistence_hpp
#define argo_persistence_hpp argo_persistence_hpp

#include "types/types.hpp"

namespace argo::backend::persistence {

	// class atomic_metadata {
	// public:
	// 	static const argo::node_id_t no_user = -1;
	// 	argo::node_id_t last_user;
	// 	std::size_t group_idx;
	// 	atomic_metadata() : last_user(no_user), group_idx(0) {};
	// 	atomic_metadata(argo::node_id_t last_user)
	// 	: last_user(last_user), group_idx(0) {};
	// 	atomic_metadata(argo::node_id_t last_user, std::size_t group_idx)
	// 	: last_user(last_user), group_idx(group_idx) {};
	// };

	// // Assumption: MPI operations are able to preserve failure atomicity for the entire struct
	// template<typename T>
	// class persistent_atomic {
	// public:
	// 	atomic_metadata metadata;
	// 	T data;
	// 	persistent_atomic() = default;
	// 	persistent_atomic(const T &data) : metadata(), data(data) {};
	// 	template<typename... Args>
	// 	persistent_atomic(const T &data, const Args... &args) : metadata(args), data(data) {};
	// };

	// Assumption: MPI operations are able to preserve failure atomicity for the entire struct (which is 64 bits)
	class lock_data {
	public:
		static const argo::node_id_t no_user = -1;
		argo::node_id_t last_user : 32;
		std::size_t group_idx : 31; // meaningless if last_user == no_user
		bool locked : 1;
		lock_data(bool locked=false, argo::node_id_t last_user=no_user, std::size_t group_idx=0)
		: last_user(last_user), group_idx(group_idx), locked(locked) {};
		bool operator==(const lock_data &other) {
			return
				this->last_user == other.last_user &&
				this->group_idx == other.group_idx &&
				this->locked == other.locked;
		}
	};

	template <
		typename location_t = argo::memory_t,
		size_t entry_size = 4096,
		size_t dirty_unit = 1
	>
	class undo_log {
	public:
		size_t initialize(size_t offset, size_t alignment) {return 0;}

		void record_changes(location_t location, char *modified_data, char *original_data = nullptr) {}

		void lock_initiate(location_t addr) {}
		void try_lock_initiate(location_t addr, std::size_t old_data, std::size_t new_data) {}
		void lock_success(location_t addr) {}
		// void inform_predecessor(std::size_t last_user) {} // Should be part of lock success
		void lock_fail(location_t addr) {}
		void unlock(location_t addr, std::size_t new_data) {}
	};

}

#endif /* argo_persistence_hpp */