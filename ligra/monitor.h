#pragma once

#include <sys/time.h>
#include <stdlib.h>
#include <unistd.h>

class mproc_t {
public:
   mproc_t(pid_t pid = 0) { proc_id = pid; }
   pid_t get_proc_id() { return proc_id; }
   void set_proc_id(pid_t pid) { proc_id = pid; }

   size_t get_vm_usage() { return vsize; }
   size_t get_rss_usage() { return rss * page_size; }
   size_t get_major_fault() { return majflt; }
   size_t get_min_fault() { return minflt; }
   size_t get_nswap() { return nswap; }

   void get_process_stat() {
      std::ifstream stat_stream("/proc/"+std::to_string(proc_id)+"/stat", std::ios_base::in);
      stat_stream >> pid >> comm >> state >> ppid >> pgrp >> session >> tty_nr
                  >> tpgid >> flags >> minflt >> cminflt >> majflt >> cmajflt
                  >> utime >> stime >> cutime >> cstime >> priority >> nice
                  >> O >> itrealvalue >> starttime >> vsize >> rss >> rsslim
                  >> startcode >> endcode >> startstack >> kstkesp >> kstkeip
                  >> signal >> blocked >> sigignore >> sigcatch >> wchan
                  >> nswap >> cnswap >> exit_signal >> processor >> rt_priority
                  >> policy >> delayacct_blkio_ticks >> guest_time >> cguest_time
                  >> start_data >> end_data >> start_brk >> arg_start 
                  >> arg_end >> env_start >> env_end >> exit_code;
      stat_stream.close();
   }

   void record_process_stat(std::string prefix = "") {
      std::string statistic_filename = "hierg_pstat.csv";
      std::ofstream ofs;
      ofs.precision(3);
      ofs.open(statistic_filename.c_str(), std::ofstream::out | std::ofstream::app );
      ofs << prefix << ","
         << "[VM]:" << get_vm_usage() * 1.0 / GB << ","
         << "[RSS]:" << get_rss_usage() * 1.0 / GB << ","
         << "[MAJ_FLT]:" << get_major_fault() << ","
         << "[MIN_FLT]:" << get_min_fault() << ","
         << "[NSWAP]:" << get_nswap() << std::endl;
   }

private:
   pid_t proc_id;

   // information in /proc/[pid]/stat
   std::string pid, comm, state, ppid, pgrp, session, tty_nr,
               tpgid, flags, cminflt, cmajflt,
               utime, stime, cutime, cstime, priority, nice,
               O, itrealvalue, starttime, rsslim, 
               startcode, endcode, startstack, kstkesp, kstkeip,
               signal, blocked, sigignore, sigcatch, wchan,
               cnswap, exit_signal, processor, rt_priority, policy,
               delayacct_blkio_ticks, guest_time, cguest_time,
               start_data, end_data, start_brk, arg_start, 
               arg_end, env_start, env_end, exit_code;

   size_t vsize;
   size_t rss;
   size_t minflt;
   size_t majflt;
   size_t nswap;
   size_t page_size = sysconf(_SC_PAGE_SIZE); // in case x86-64 is configured to use 2MB pages
};

