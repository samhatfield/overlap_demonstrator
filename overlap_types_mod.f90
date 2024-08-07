module overlap_types_mod
    use linked_list_m, only: LinkedList, LinkedListNode
    use common_data, only: max_comms, stat_waiting, stat_pending, ncomm_started
    use common_mpi, only: ntasks, mytask, numsend, numrecv, sendbuf1, sendbuf2, recvbuf, off, &
        & recv_reqs, send_reqs

    implicit none
    private

    type, public :: Batch
        integer :: stage
        ! 1: Initialized trgtol, ready for GtoL comm
        ! 2: Completed FFT, ready for LtoM comm
        ! 3: Done
        integer :: status
        integer :: id
        integer :: nops=1
    contains
        procedure :: execute => batch_execute
        procedure :: start_comm
        procedure :: finish_comm
        procedure :: comm_complete
    end type Batch

    interface Batch
        module procedure :: batch_constructor
    end interface Batch

    type, public, extends(LinkedList) :: BatchList
    contains
        procedure :: append => append_batch
    end type BatchList

    type(BatchList), public :: active_batches

contains

    ! -----------------------------------------------------------------------------
    ! BatchList and CommList methods
    ! -----------------------------------------------------------------------------

    !> Add a value to the list at the tail
    subroutine append_batch(this, value)
        class(BatchList), intent(inout) :: this
        class(*), intent(in), target    :: value

        type(LinkedListNode), pointer :: node_ptr, next_ptr, current_ptr

        ! Create a new node and set the value
        allocate(node_ptr)
        allocate(node_ptr%value, source=value)
        node_ptr%next => null()
        this%size = this%size + 1

        if (.not. associated(this%head))then
           this%head => node_ptr
           this%tail => node_ptr
        else
           this%tail%next => node_ptr
           node_ptr%prev  => this%tail
           this%tail      => node_ptr
        end if

    end subroutine append_batch


    ! -----------------------------------------------------------------------------
    ! Batch methods
    ! -----------------------------------------------------------------------------

    function batch_constructor(batch_index) result(this)
        integer, intent(in) :: batch_index
        integer :: i, itask
        
        type(Batch) :: this

        this%stage = 1
        this%status = stat_waiting
        this%id = batch_index

        do itask=1,ntasks
            do i=1,numsend
                sendbuf1(i+off(itask),this%id) = this%id * 0.0001 + i + off(itask)
            enddo
        enddo
        
        this%nops = 1

    end function batch_constructor

    subroutine batch_execute(this)
        class(Batch), intent(inout), target :: this
        real :: r
        integer(8) :: i, j
        
        write(*,*) "Rank", mytask, "Batch", this%id, "executing stage ",this%stage

        select case (this%stage)
        case (1)
            ! Do FFT
            call random_number(r)
            do i=1,r*10000
                j = j + i*i
            enddo
            ! Start communication or set as pending
            if (ncomm_started < max_comms) then
                ncomm_started = ncomm_started + 1
                write(*,*) "Rank", mytask, 'Starting comm. ',this%id, ', stage ',this%stage+1
                call this%start_comm(this%stage+1)
                this%status = stat_waiting
            else
                write(*,*) "Rank", mytask, 'Batch ',this%id, ', stage ',this%stage+1,' is pending'
                this%status = stat_pending
            endif
        case(2)
            ! Do Legendre
            call random_number(r)
            do i=1,r*40000
                j = j + i*i
            enddo
        case default
        end select

        this%stage = this%stage+1
        ! Just set status to final for now
        !this%stage = 2
    end subroutine batch_execute

    ! -----------------------------------------------------------------------------
    ! Comm methods
    ! -----------------------------------------------------------------------------

    function comm_complete(this)
        use mpi, only: mpi_testall, mpi_statuses_ignore

        class(Batch), intent(in) :: this
        logical :: comm_complete
        integer ierr
        
        ! Test whether this comm has finished yet (just always true for now)
        call mpi_testall(this%nops,recv_reqs(:,this%id),comm_complete,mpi_statuses_ignore,ierr)
    end function comm_complete

    subroutine start_comm(this,stage)
        use common_data
        use mpi, only: mpi_isend, mpi_recv, mpi_real, mpi_float, mpi_comm_world, mpi_ialltoall
      
        class(Batch), intent(inout) :: this
        integer, intent(in) :: stage
        integer :: i, isource, idest, ierr

        select case(stage)
        case (1)
            do i=1,ntasks-1
                idest = mod(mytask+i,ntasks)
                call mpi_isend(sendbuf1(off(idest),this%id),numsend,MPI_REAL,idest, &
                    & mytask,mpi_comm_world,send_reqs(i,this%id),ierr)
                isource = mod(mytask-i+ntasks,ntasks)
                call mpi_irecv(recvbuf(off(isource),this%id),numrecv,MPI_REAL,isource,isource, &
                    & mpi_comm_world,recv_reqs(i,this%id),ierr)
            enddo
            this%nops = ntasks-1
        case (2)
            call mpi_ialltoall(sendbuf2(1,this%id),numsend,MPI_REAL, &
                & recvbuf(1,this%id),numrecv,MPI_FLOAT,mpi_comm_world,recv_reqs(1,this%id),ierr)
        case default
            write(*,*) 'ERROR: incorrect stage',stage
        end select
    end subroutine start_comm

    subroutine finish_comm(this)
        use mpi

        class(Batch), intent(in) :: this
        integer :: ierr,i
        
!        call mpi_waitall(this%nops,recv_reqs(:,this%id),mpi_statuses_ignore,ierr)
!        if(this%nops > 1) then
!           call mpi_waitall(this%nops,send_reqs(:,this%id),mpi_statuses_ignore,ierr)
!        endif

        if(this%stage .eq. 1) then
            do i=1,numsend
                sendbuf2(i,this%id) = recvbuf(i,this%id)
            enddo
        endif
    end subroutine finish_comm

end module overlap_types_mod
