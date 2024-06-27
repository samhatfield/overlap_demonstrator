module common_data
  use mpi
  
  integer ntasks
  integer mytask
  integer numsend
  integer numrecv

  real(4), allocatable :: sendbuf(:,:),recvbuf(:,:)
  integer, allocatable :: off(:)
!  type(mpi_request), allocatable :: recv_reqs(:,:),send_reqs(:,:)
  integer, allocatable :: recv_reqs(:,:),send_reqs(:,:)
  
end module common_data
