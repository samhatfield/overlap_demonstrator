module overlap_types_mod
    use linked_list_m, only: LinkedList, LinkedListNode

    implicit none
    private

    integer, public, parameter :: stat_waiting = 1
    integer, public, parameter :: stat_pending = 2

    type, public :: Batch
        integer :: stage
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
        type(Batch), pointer :: my_batch
    contains
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

        type(Batch) :: this

        this%stage = 1
        this%status = stat_waiting
        this%id = batch_index
    end function batch_constructor

    subroutine batch_associate_comm(this, my_comm)
        class(Batch), intent(inout) :: this
        class(Comm), intent(in), target :: my_comm

        this%my_comm => my_comm
    end subroutine batch_associate_comm

    subroutine batch_execute(this)
        class(Batch), intent(inout) :: this

        ! Just set status to final for now
        write(*,*) "Batch", this%id, "executing"
        this%stage = 2
    end subroutine batch_execute

    ! -----------------------------------------------------------------------------
    ! Comm methods
    ! -----------------------------------------------------------------------------

    function comm_constructor(comm_index) result(this)
        integer, intent(in)             :: comm_index

        type(Comm) :: this

        this%id = comm_index
    end function comm_constructor

    function comm_complete(this)
        class(Comm), intent(in) :: this
        logical :: comm_complete

        ! Test whether this comm has finished yet (just always true for now)
        write(*,*) "Checking completion of comm", this%id
        comm_complete = .true.
    end function comm_complete

end module overlap_types_mod
