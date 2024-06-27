module overlap_types_mod
    use linked_list_m, only: LinkedList, LinkedListNode
    use common_data
    use mpi
    
    implicit none
    private

    integer, public, parameter :: stat_waiting = 1
    integer, public, parameter :: stat_pending = 2

    type, public :: Batch
        integer :: stage
        ! 1: Initialized trgtol, ready for GtoL comm
        ! 2: Completed FFT, ready for LtoM comm
        ! 3: Done
        integer :: status
        integer :: id
        type(Comm), pointer :: my_comm
    contains
        procedure :: associate_comm => batch_associate_comm
        procedure :: execute => batch_execute
    end type Batch

    interface Batch
        module procedure :: batch_constructor
    end interface Batch

    type, public :: Comm
        integer :: id
        integer :: nops=1
        type(Batch), pointer :: my_batch
    contains
      procedure :: start
      procedure :: finish
      procedure :: complete => comm_complete
        ! procedure :: get_batch
    end type Comm

    interface Comm
        module procedure :: comm_constructor
    end interface Comm

    type, public, extends(LinkedList) :: BatchList
    contains
        procedure :: append => append_batch
    end type BatchList

    type, public, extends(LinkedList) :: CommList
    contains
        procedure :: append => append_comm
    end type CommList

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

    !> Add a value to the list at the tail
    subroutine append_comm(this, value)
        class(CommList), intent(inout) :: this
        class(*), intent(in), target   :: value

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

    end subroutine append_comm

    ! -----------------------------------------------------------------------------
    ! Batch methods
    ! -----------------------------------------------------------------------------

    function batch_constructor(batch_index) result(this)
        integer, intent(in) :: batch_index
        integer i,itask
        
        type(Batch) :: this

        this%stage = 1
        this%status = stat_waiting
        this%id = batch_index

        do itask=1,ntasks
           do i=1,numsend
              sendbuf(i+off(itask),this%id) = this%id * 0.0001 + i + off(itask)
           enddo
        enddo
        
    end function batch_constructor

    subroutine batch_associate_comm(this, my_comm)
        class(Batch), intent(inout) :: this
        class(Comm), intent(in), target :: my_comm

        this%my_comm => my_comm
    end subroutine batch_associate_comm

    subroutine batch_execute(this)
        class(Batch), intent(inout) :: this
        real r
        integer i,j
        
        write(*,*) "Batch", this%id, "executing"

        select case (this%stage)
        case (1)
           ! Do FFT
           call random_number(r)
           do i=1,r*10000
              j = j + i*i
           enddo
           print *,j
        case(2)
           ! Do Legendre
           call random_number(r)
           do i=1,r*40000
              j = j + i*i
           enddo
           print *,j
        case default
        end select

        this%stage = this%stage+1
        ! Just set status to final for now
        !this%stage = 2
    end subroutine batch_execute

    ! -----------------------------------------------------------------------------
    ! Comm methods
    ! -----------------------------------------------------------------------------

    function comm_constructor(comm_index) result(this)
        integer, intent(in) :: comm_index
        type(Comm) :: this
        
        this%id = comm_index
        this%nops = 1
    end function comm_constructor

    function comm_complete(this)
      use mpi
      
      class(Comm), intent(in) :: this
        logical :: comm_complete
        integer ierr,flg
        
        ! Test whether this comm has finished yet (just always true for now)
        write(*,*) "Checking completion of comm", this%id
        call mpi_testall(this%nops,recv_reqs(:,this%id),comm_complete,mpi_statuses_ignore,ierr)
      end function comm_complete

      subroutine start(this,stage)
        use common_data
        use mpi
        implicit none
      
        class(Comm), intent(inout) :: this
        integer, intent(in) :: stage
        integer i,isource,idest,ierr

        select case(stage)
        case (1)
           do i=1,ntasks-1
              idest = mod(mytask+i,ntasks)
              call mpi_isend(sendbuf(off(idest),this%id),numsend,MPI_REAL,idest, &
                   mytask,mpi_comm_world,send_reqs(i,this%id),ierr)
              isource = mod(mytask-i+ntasks,ntasks)
              call mpi_irecv(recvbuf(off(isource),this%id),numrecv,MPI_REAL,isource,isource, &
                   mpi_comm_world,recv_reqs(i,this%id),ierr)
           enddo
           this%nops = ntasks-1
        case (2)
           call mpi_ialltoall(sendbuf(1,this%id),numsend,MPI_REAL, &
                   recvbuf(1,this%id),numrecv,MPI_FLOAT,mpi_comm_world,recv_reqs(1,this%id),ierr)
        case default
           print *,'ERROR: incorrect stage',stage
        end select
        
      end subroutine start
      
      subroutine finish(this)
        use mpi
        implicit none

        class(Comm), intent(in) :: this
        integer ierr
        
        call mpi_waitall(this%nops,recv_reqs(:,this%id),mpi_statuses_ignore,ierr)
        if(this%nops > 1) then
           call mpi_waitall(this%nops,send_reqs(:,this%id),mpi_statuses_ignore,ierr)
        endif
        
    end subroutine finish
    
end module overlap_types_mod
