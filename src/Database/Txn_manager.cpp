#include "Txn_manager.h"
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <unordered_map>
#include <unordered_set>
#include <unistd.h>


Txn_manager::Txn_manager(Database *db, string db_name)
{
	this->db = db;
	this->db_name = db_name;
	this->log_path = GlobalTypedef::db_path(db_name) + "/update.log";
	this->all_log_path = GlobalTypedef::db_path(db_name) + "/update_since_backup.log";
	out.open(this->log_path.c_str(), ios::out | ios::app);
	out_all.open(this->all_log_path.c_str(), ios::out | ios::app);
	wal_fd = ::open(this->log_path.c_str(), O_CREAT | O_APPEND | O_WRONLY, 0644);
	if (wal_fd < 0)
	{
		SLOG_ERROR("open wal failed: " << strerror(errno));
	}
	cnt.store(1);
	txn_table.clear();
	for(int i = 0; i < 3; i++)
	{
		IDSet s;
		this->DirtyKeys.push_back(s);
	}
}

Txn_manager::~Txn_manager()
{
	abort_all_running();
	Checkpoint();
	SLOG_CORE("Checkpoint done");
	txn_table.clear();
	out.close();
	out_all.close();
	if (wal_fd >= 0)
	{
		::close(wal_fd);
		wal_fd = -1;
	}
}

void Txn_manager::writelog(string str)
{
	this->lock_log();
	out << str << endl;
	out_all << str << endl;
	this->unlock_log();
}

string Txn_manager::wal_type_to_string(WalType t) const
{
	switch (t)
	{
	case WalType::BEGIN:
		return "BEGIN";
	case WalType::UPDATE:
		return "UPDATE";
	case WalType::COMMIT:
		return "COMMIT";
	case WalType::ABORT:
		return "ABORT";
	case WalType::CHECKPOINT:
		return "CHECKPOINT";
	default:
		return "UNKNOWN";
	}
}

string Txn_manager::encode_wal(const WalRecord &rec) const
{
	string payload = rec.payload;
	for (auto &ch : payload)
	{
		if (ch == '\n' || ch == '\r' || ch == '\t')
		{
			ch = ' ';
		}
	}
	return to_string(rec.ts) + "\t" + to_string(rec.tid) + "\t" + wal_type_to_string(rec.type) + "\t" + payload + "\n";
}

bool Txn_manager::append_wal(const WalRecord &rec, bool force_sync)
{
	string line = encode_wal(rec);
	lock_guard<mutex> lk(log_lock);

	if (wal_fd < 0)
	{
		wal_fd = ::open(this->log_path.c_str(), O_CREAT | O_APPEND | O_WRONLY, 0644);
		if (wal_fd < 0)
		{
			SLOG_ERROR("open wal failed: " << strerror(errno));
			return false;
		}
	}

	ssize_t n = ::write(wal_fd, line.data(), line.size());
	if (n != static_cast<ssize_t>(line.size()))
	{
		SLOG_ERROR("write wal failed: " << strerror(errno));
		return false;
	}

	out_all << line;
	out_all.flush();

	if (force_sync && ::fsync(wal_fd) != 0)
	{
		SLOG_ERROR("fsync wal failed: " << strerror(errno));
		return false;
	}
	return true;
}

bool Txn_manager::rotate_wal_after_restore(size_t parsed_lines, size_t replay_fail_cnt)
{
	if (parsed_lines == 0 || replay_fail_cnt > 0)
	{
		return false;
	}

	lock_guard<mutex> lk(log_lock);
	if (wal_fd >= 0)
	{
		::close(wal_fd);
		wal_fd = -1;
	}
	if (out.is_open())
	{
		out.close();
	}

	string archive_path = this->log_path + ".recovered." + to_string(gs::TimeUtil::timestamp());
	if (::rename(this->log_path.c_str(), archive_path.c_str()) != 0)
	{
		SLOG_WARN("rotate wal failed: " << strerror(errno));
	}
	else
	{
		SLOG_INFO("rotate wal success, archived at " << archive_path);
	}

	out.open(this->log_path.c_str(), ios::out | ios::app);
	wal_fd = ::open(this->log_path.c_str(), O_CREAT | O_APPEND | O_WRONLY, 0644);
	if (wal_fd < 0)
	{
		SLOG_ERROR("reopen wal failed: " << strerror(errno));
		return false;
	}
	return true;
}

bool Txn_manager::add_transaction(txn_id_t TID, shared_ptr<Transaction> txn)
{
	//assert(txn_table.find(TID) == txn_table.end());
	table_lock.lockExclusive();
	txn_table.insert(pair<txn_id_t,shared_ptr<Transaction> >(TID, txn));
	table_lock.unlock();
	return true;
}

shared_ptr<Transaction> Txn_manager::get_transaction(txn_id_t TID)
{
	table_lock.lockShared();
	if(txn_table.find(TID) == txn_table.end()) {
		table_lock.unlock();
		SLOG_ERROR("wrong TID");
		return nullptr;
	}
	auto p =  txn_table[TID];
	table_lock.unlock();
	return p;
}

//TODO: undo failed
bool Txn_manager::undo(string str, txn_id_t TID)
{
	Util::get_timestamp(str);
	string undo_sparql;
	if (str[0] == 'I') {
		undo_sparql = "DELETE DATA{";
	}
	else if (str[0] == 'D') {
		undo_sparql = "INSERT DATA{";
	}
	else {
		SLOG_ERROR("wrong undo sparql: " << str);
		return false;
	}
	undo_sparql += str.substr(2, str.length());
	undo_sparql += '}';
	ResultSet rs;
	FILE* output = stdout;
	shared_ptr<Transaction> txn = get_transaction(TID);
	if(db != nullptr)
		this->db->query(undo_sparql, rs, output);
	else
	{
		SLOG_ERROR("error! database has been flushed or removed!");
		return false;
	}
	return true;
}

bool Txn_manager::redo(string str, txn_id_t TID)
{
	Util::get_timestamp(str);
	string redo_sparql;
	if (str[0] == 'I') {
		redo_sparql = "INSERT DATA{";
	}
	else if (str[0] == 'D') {
		redo_sparql = "DELETE DATA{";
	}
	else {
		SLOG_ERROR("wrong redo sparql: " << str);
		return false;
	}
	redo_sparql += str.substr(2, str.length());
	redo_sparql += '}';
	ResultSet rs;
	FILE* output = stdout;
	shared_ptr<Transaction> txn = get_transaction(TID); 
	if(db != nullptr)
		this->db->query(redo_sparql, rs, output);
	else
	{
		SLOG_CORE("error! database has been flushed or removed");
		return false;
	}
	return true;
}


/*
inline txn_id_t Txn_manager::ArrangeTID()
{
	srand(time(NULL));
	cnt++;
	return stod(gs::TimeUtil::timestamp_str()) * 10000 + (rand() + cnt) % 10000 ;
}
*/

inline txn_id_t Txn_manager::ArrangeTID()
{
	txn_id_t TID = cnt.fetch_add(1);
	if(TID == INVALID_ID)
	{
		SLOG_ERROR("TID wrapped! ");
	}
	return TID;
}

inline txn_id_t Txn_manager::ArrangeCommitID()
{
	return cnt.load();
}

txn_id_t Txn_manager::Begin(IsolationLevelType isolationlevel)
{
	checkpoint_lock.lockShared();
	txn_id_t TID = this->ArrangeTID();
	if(TID == INVALID_ID)
	{
		SLOG_CORE("TID wrapped, please run garbage clean!");
		checkpoint_lock.unlock();
		return TID;
	}
	shared_ptr<Transaction> txn = make_shared<Transaction>(this->db_name, gs::TimeUtil::timestamp(), TID, isolationlevel);
	txn->SetCommitID(TID);
	add_transaction(TID, txn);
	WalRecord rec{TID, WalType::BEGIN, gs::TimeUtil::timestamp(), ""};
	if (!append_wal(rec, false))
	{
		txn->SetState(TransactionState::ABORTED);
		checkpoint_lock.unlock();
		return INVALID_ID;
	}
	txn->SetState(TransactionState::RUNNING);
	return TID;
}

int Txn_manager::Commit(txn_id_t TID)
{
	shared_ptr<Transaction> txn = get_transaction(TID);
	if (txn == nullptr) {
		SLOG_ERROR("wrong transaction id!");
		// checkpoint_lock.unlock();
		return -1;
	}
	else if (txn->GetState() != TransactionState::RUNNING) {
		SLOG_ERROR("transaction not in running state! commit failed" << " " << (int)txn->GetState());	
		// checkpoint_lock.unlock();
		return 1;
	}
	txn_id_t CID = this->ArrangeCommitID();
	txn->SetCommitID(CID);
	WalRecord rec{TID, WalType::COMMIT, gs::TimeUtil::timestamp(), to_string(CID)};
	if (!append_wal(rec, true))
	{
		txn->SetState(TransactionState::ABORTED);
		checkpoint_lock.unlock();
		return -2;
	}
	if(db != nullptr)
		db->TransactionCommit(txn);
	else
	{
		SLOG_CORE("error! database has been flushed or removed");
		// checkpoint_lock.unlock();
		return -1;
	}
	txn->SetState(TransactionState::COMMITTED);
	txn->SetEndTime(gs::TimeUtil::timestamp());
	add_dirty_keys(txn);
	checkpoint_lock.unlock();
	committed_num++;
	int cycle = 50000;
	if(committed_num.compare_exchange_strong(cycle, 0)){
		Checkpoint();
		SLOG_ERROR("checkpoint done!");
	}
	return 0;
}

int Txn_manager::Abort(txn_id_t TID)
{
	shared_ptr<Transaction> txn = get_transaction(TID);
	if (txn == nullptr) {
		SLOG_ERROR("wrong transaction id!");
		return -1;
	}
	if(db != nullptr)
		db->TransactionRollback(txn);
	else
	{
		SLOG_CORE("error! database has been flushed or removed");
		return -1;
	}
	WalRecord rec{TID, WalType::ABORT, gs::TimeUtil::timestamp(), ""};
	append_wal(rec, false);
	txn->SetState(TransactionState::ABORTED);
	txn->SetEndTime(gs::TimeUtil::timestamp());
	checkpoint_lock.unlock();
	//add_dirty_keys(txn);
	return 0;
}

int Txn_manager::Rollback(txn_id_t TID)
{
	shared_ptr<Transaction> txn = get_transaction(TID);
	if (txn == nullptr) {
		SLOG_ERROR("wrong transaction id!");
		return -1;
	}
	else if (txn->GetState() != TransactionState::RUNNING) {
		SLOG_ERROR("transaction not in running state! rollback failed");
		return 1;
	}
	return Abort(TID);
}

int Txn_manager::Query(txn_id_t TID, string sparql, string& results)
{
	shared_ptr<Transaction> txn = get_transaction(TID);
	if(txn == nullptr)
	{
		results ="wrong transaction ID!";
		return -1;
	}
	if (txn->GetState() != TransactionState::RUNNING) {
		results = "transaction not in running state!";
		return -99;
	}
	try
	{
		QueryTree::UpdateType update_type;
		if (db != nullptr && db->isUpdate(sparql, update_type) && update_type != QueryTree::Not_Update)
		{
			WalRecord rec{TID, WalType::UPDATE, gs::TimeUtil::timestamp(), sparql};
			if (!append_wal(rec, false))
			{
				txn->SetState(TransactionState::ABORTED);
				results = "write wal update failed";
				return -30;
			}
		}
	}
	catch (const std::exception &e)
	{
		SLOG_WARN("txn update detect failed: " << e.what());
	}

	int ret_val;
	ResultSet rs;
	FILE* output = stdout;
	if(db != nullptr)
		ret_val = this->db->query(sparql, rs, output , true, false, txn);
	else{
		results = "database has been flushed or removed!";
		return -10;
	}
	if(txn->GetState() == TransactionState::ABORTED)
	{
		results = "transaction in aborted state!";
		Abort(TID);
		return -20;
	}
	if (ret_val < -1)   //non-update query
	{
		rs.to_JSON(results);
		return ret_val;
	}
	else
	{
		get_transaction(TID)->update_num += ret_val;
		return ret_val;
	}
}

void Txn_manager::Checkpoint()
{
	SLOG_CORE("set checkpoint_lock lockExclusive begin...");
	checkpoint_lock.lockExclusive();
	WalRecord rec{INVALID_TID, WalType::CHECKPOINT, gs::TimeUtil::timestamp(), db_name};
	append_wal(rec, false);
	SLOG_CORE("set checkpoint_lock lockExclusive ok.");
	vector<unsigned> sub_ids , obj_ids, obj_literal_ids, pre_ids;
	sub_ids.insert(sub_ids.begin(), DirtyKeys[0].begin(), DirtyKeys[0].end());
	pre_ids.insert(pre_ids.begin(), DirtyKeys[1].begin(), DirtyKeys[1].end());
	for(auto key: DirtyKeys[2])
	{
		if(Util::is_entity_ele(key)){
			obj_ids.push_back(key);
		}
		else{
			obj_literal_ids.push_back(key);
		}
	}
 	if(db != nullptr)
		db->VersionClean(sub_ids, obj_ids, obj_literal_ids, pre_ids);
	checkpoint_lock.unlock();
}


//TODO:this function is not complete
void Txn_manager::restore()
{
	ifstream in(this->log_path.c_str(), ios::in);
	if (!in.is_open())
	{
		SLOG_WARN("restore skip: can not open wal file " << this->log_path);
		return;
	}

	vector<pair<txn_id_t, string> > update_events;
	unordered_map<txn_id_t, bool> txn_committed;
	size_t parsed_lines = 0;
	size_t malformed_lines = 0;
	string line;
	while (getline(in, line))
	{
		parsed_lines++;
		if (line.empty())
		{
			continue;
		}

		vector<string> cols;
		Util::split(line, "\t", cols);
		if (cols.size() < 3)
		{
			malformed_lines++;
			continue;
		}

		try
		{
			txn_id_t tid = strtoull(cols[1].c_str(), nullptr, 10);
			const string &type = cols[2];
			if (type == "UPDATE")
			{
				if (cols.size() >= 4 && !cols[3].empty())
				{
					update_events.push_back(make_pair(tid, cols[3]));
				}
			}
			else if (type == "COMMIT")
			{
				txn_committed[tid] = true;
			}
			else if (type == "ABORT")
			{
				txn_committed[tid] = false;
			}
		}
		catch (const std::exception &e)
		{
			malformed_lines++;
			SLOG_WARN("restore parse wal line failed: " << e.what());
		}
	}

	if (db == nullptr)
	{
		SLOG_ERROR("restore failed: database pointer is null");
		return;
	}

	size_t replay_cnt = 0;
	size_t replay_fail_cnt = 0;
	for (const auto &event : update_events)
	{
		txn_id_t tid = event.first;
		auto state_it = txn_committed.find(tid);
		if (state_it == txn_committed.end() || state_it->second == false)
		{
			continue;
		}
		const string &sparql = event.second;
		try
		{
			ResultSet rs;
			int ret = db->query(sparql, rs, nullptr, true, false, nullptr);
			if (ret < 0)
			{
				replay_fail_cnt++;
				SLOG_WARN("restore replay update failed, tid=" << tid << ", ret=" << ret);
			}
			else
			{
				replay_cnt++;
			}
		}
		catch (const std::exception &e)
		{
			replay_fail_cnt++;
			SLOG_WARN("restore replay update exception, tid=" << tid << ", msg=" << e.what());
		}
	}
	SLOG_INFO("restore finished for db=" << db_name
			  << ", wal_lines=" << parsed_lines
			  << ", malformed_lines=" << malformed_lines
			  << ", replayed_updates=" << replay_cnt
			  << ", replay_failed=" << replay_fail_cnt);
	rotate_wal_after_restore(parsed_lines, replay_fail_cnt);
}

txn_id_t Txn_manager::find_latest_txn()
{
	auto it = txn_table.begin();
	if (it == txn_table.end()) {
		return 0;
	}
	txn_id_t max = it->first;
	bool is_found = false;
	if (it->second->GetState() == TransactionState::RUNNING)
	{
		is_found = true;
	}
	for (; it != txn_table.end(); it++)
	{
		if (it->second->GetStartTime() > txn_table[max]->GetStartTime() && it->second->GetState() == TransactionState::RUNNING)
		{
			max = it->first;
			is_found = true;
		}
	}
	if (is_found)
		return max;
	else
		return 0;
}

void Txn_manager::abort_all_running()
{
	auto it = txn_table.begin();
	for (; it != txn_table.end(); it++)
	{
		if (it->second->GetState() == TransactionState::RUNNING)
		{
			Abort(it->first);
		}
	}
}

void Txn_manager::print_txn_dataset(txn_id_t TID)
{
	auto txn = Get_Transaction(TID);
	txn->print_all();
}

void Txn_manager::add_dirty_keys(shared_ptr<Transaction> txn)
{
	//lock_guard<mutex> lck(DirtyKeys_lock);
	DirtyKeys_lock.lock();
	const vector<IDSet> & sets = txn->Get_WriteSet();
	for(int i = 0; i < 3; i++)
	{
		DirtyKeys[i].insert(sets[i].begin(), sets[i].end());
	}
	DirtyKeys_lock.unlock();
}