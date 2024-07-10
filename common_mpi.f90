module common_mpi  
  integer :: ntasks
  integer :: mytask
  integer :: numsend
  integer :: numrecv

  real(4), allocatable :: sendbuf1(:,:),sendbuf2(:,:),recvbuf(:,:)
  integer, allocatable :: off(:)
!  type(mpi_request), allocatable :: recv_reqs(:,:),send_reqs(:,:)
  integer, allocatable :: recv_reqs(:,:),send_reqs(:,:)

end module common_mpi
