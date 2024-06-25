module overlap_types_mod
    use linked_list_m, only: LinkedListNode

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
        type(Batch), pointer :: my_batch
    contains
        procedure :: complete => comm_complete
        ! procedure :: get_batch
    end type Comm

    interface Comm
        module procedure :: comm_constructor
    end interface Comm
contains

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

        ! Do nothing for now
    end subroutine batch_execute

    ! -----------------------------------------------------------------------------
    ! Comm methods
    ! -----------------------------------------------------------------------------

    function comm_constructor(my_batch) result(this)
        type(Batch), intent(in), target :: my_batch

        type(Comm) :: this

        this%my_batch => my_batch
    end function comm_constructor

    function comm_complete(this)
        class(Comm), intent(in) :: this
        logical :: comm_complete

        ! Test whether this comm has finished yet (just always true for now)
        comm_complete = .true.
    end function comm_complete

end module overlap_types_mod