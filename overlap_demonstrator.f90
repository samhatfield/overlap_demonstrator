program overlap_demonstrator
    use linked_list_m, only: LinkedList, LinkedListNode
    use overlap_types_mod, only: Batch, Comm, stat_waiting, stat_pending

    implicit none

    type(LinkedList) :: active_comms
    type(LinkedList) :: active_batches

    integer, parameter :: nbatches = 10
    integer, parameter :: max_comms = 5
    integer, parameter :: max_active_batches = 5
    integer, parameter :: stage_final = 2
    logical :: comm_compl
    logical :: productive

    integer :: nactive
    integer :: ndone
    integer :: ncomm_started
    type(LinkedListNode), pointer :: ic
    type(LinkedListNode), pointer :: ib
    type(Batch), pointer :: this_batch

    ! Activate first batch
    call activate(1)
    ! if (associated(active_batches%head)) write(*,*) "head is active"
    ! if (.not. associated(active_batches%head%next)) write(*,*) "head has no next"
    ! if (.not. associated(active_batches%tail%next)) write(*,*) "tail has no next"
    call activate(2)
    ! if (associated(active_batches%head)) write(*,*) "head is active"
    ! if (.not. associated(active_batches%head%next)) write(*,*) "head has no next"
    ! if (.not. associated(active_batches%tail%next)) write(*,*) "tail has no next"

    call active_batches%traverse(print_ids)

    !ic => active_comms%head

    nactive = 1
    ndone = 0
    ncomm_started = 0

    ! Keep looping until all batches are complete
    do while (ndone < nbatches)
        ! Check whether any active communications have completed
        comm_compl = .false.
        productive = .false.
        ic = active_comms%first()
        do while (associated(ic))
          select type (thisComm => ic%value)
          type is (Comm)
            if (thisComm%complete()) then
                ! complete_comm(ic%value) step needed here?
                this_batch = thisComm%my_batch
!! remove() method not yet implemented?
!!                call active_comms%remove(ic)
                comm_compl = .true.
                ncomm_started = ncomm_started - 1
                exit
            end if
            ic = ic%next
          end select
        end do
        
        if (comm_compl) then
            ! Now that one comm has completed, we can start the comm on the next batch whose comm is
            ! pending, if there is one
            ib = active_batches%first()
            do while (associated(ib))
              select type (thisBatch => ib%value)
              type is (Batch)
                if (thisBatch%status == stat_pending) then
                    !call start(ib%value%comm_dep)
                    ncomm_started = ncomm_started + 1
                    exit
                end if
                ib = ib%next
              end select
            end do

            productive = .true.
            call this_batch%execute
            if (this_batch%stage == stage_final) then
                ! If batch has completed, remove it from the active batches list (requires a search)
                ib = active_batches%first()
                do while (associated(ib))
                  select type (listBatch => ib%value)
                  type is (Batch)
                    if (listBatch%id == this_batch%id) then
!! remove() method not yet implemented?
!!                        call active_batches%remove(ib)
                        exit
                    end if
                    ib = ib%next
                  end select
                end do
                nactive = nactive - 1
                ndone = ndone + 1
            end if
        else
            ib = active_batches%first()
            do while (associated(ib))
              select type (thisBatch =>ib%value)
              type is (Batch)
                if (thisBatch%status /= stat_waiting) then
                    productive = .true.
                    call thisBatch%execute
                    if (thisBatch%stage == stage_final) then
!! remove() method not yet implemented?
!!                        call active_batches%remove(ib)
                        nactive = nactive - 1
                        ndone = ndone + 1
                        if (.not. associated(active_batches%first())) then
                            exit
                        end if
                    end if
                end if
                ib = ib%next
              end select
            end do

            if (.not. productive) then
                if (nactive < max_active_batches) then
                    nactive = nactive + 1
                    call activate(nactive)
                end if
            end if
        end if
    end do

contains

    subroutine activate(n)
        integer, intent(in) :: n

        type(Batch) :: new_batch
        type(Comm)  :: new_comm

        new_batch = Batch(n)
        new_comm = Comm(new_batch)
        call new_batch%associate_comm(new_comm)

        if (ncomm_started < max_comms) then
            ncomm_started = ncomm_started + 1
            !call start(new_comm)
            new_batch%status = stat_waiting
        else
            new_batch%status = stat_pending
        endif

        call active_comms%append(new_comm)
        call active_batches%append(new_batch)
    end subroutine activate

    subroutine print_ids(node)
        class(LinkedListNode), pointer, intent(inout) :: node

        select type(p => node)
            type is(Batch)
                write(*,*) p%id
            type is(Comm)
                write(*,*) "Got Comm"
        class default
            write(*,*) "ERROR"
        end select
    end subroutine print_ids

end program overlap_demonstrator
