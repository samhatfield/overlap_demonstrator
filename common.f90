module common_data

  integer, public, parameter :: nbatches = 10
  integer, public, parameter :: max_comms = 2
  integer, public, parameter :: max_active_batches = 3
  integer, public, parameter :: stage_final = 3
  integer, public, parameter :: stat_waiting = 1
  integer, public, parameter :: stat_pending = 2
  integer, public, parameter :: stat_exec = 3
  integer :: ncomm_started
  integer :: nactive
  
end module common_data
