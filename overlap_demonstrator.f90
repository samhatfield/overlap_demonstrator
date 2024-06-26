program overlap_demonstrator
    use linked_list_m  , only: LinkedList, LinkedListNode
    use overlap_types_mod , only: Batch, Comm, stat_waiting, stat_pending, CommList, BatchList

    implicit none

    type(CommList) :: active_comms
    type(BatchList) :: active_batches

    integer, parameter :: nbatches = 10
    integer, parameter :: max_comms = 5
    integer, parameter :: max_active_batches = 5
    integer, parameter :: stage_final = 2
    logical :: comm_compl
    logical :: productive

    integer :: nactive
    integer :: ndone
    integer :: ncomm_started
    integer :: ncurrent
    type(LinkedListNode), pointer :: ic
    type(LinkedListNode), pointer :: ib
    type(Batch), pointer :: complete_comm_batch

    ! Test everything works as expected
    write(*,*) "Testing BatchList/CommList functionality"

    ! Activate three batches/comms
    write(*,*) "Call activate 3 times"
    call activate(1)
    call activate(2)
    call activate(3)

    ! Print IDs of all active batches
    write(*,*) "Print all batch IDs"
    call active_batches%traverse(print_ids)

    ! Remove middle batch
    write(*,*) "Remove middle batch"
    ib => active_batches%head%next
    call active_batches%remove(ib)

    ! Print IDs again
    write(*,*) "Print all batch IDs again"
    call active_batches%traverse(print_ids)

    ! Remove all batches
    write(*,*) "Reset"
    call active_batches%reset
    call active_comms%reset

    ! Now start the actual work
    write(*,*)
    write(*,*) "Now start the actual work"

    nactive = 1
    ndone = 0
    ncomm_started = 0
    ncurrent = 1

    call activate(ncurrent)

    ! Keep looping until all batches are complete
    do while (ndone < nbatches)
        ! Check whether any active communications have completed
        comm_compl = .false.
        productive = .false.
        ic => active_comms%head
        do while (associated(ic))
            select type (thisComm => ic%value)
            type is (Comm)
                if (thisComm%complete()) then
                    ! complete_comm(ic%value) step needed here?
                    complete_comm_batch => thisComm%my_batch
                    write(*,*) "Comm", thisComm%id, " complete"
                    call active_comms%remove(ic)
                    comm_compl = .true.
                    ncomm_started = ncomm_started - 1
                    exit
                end if
                ic => ic%next
            end select
        end do
        
        if (comm_compl) then
            ! Now that one comm has completed, we can start the comm on the next batch whose comm is
            ! pending, if there is one
            ib => active_batches%head
            do while (associated(ib))
                select type (thisBatch => ib%value)
                type is (Batch)
                    if (thisBatch%status == stat_pending) then
                        !call start(ib%value%comm_dep)
                        ncomm_started = ncomm_started + 1
                        exit
                    end if
                    ib => ib%next
                end select
            end do

            productive = .true.
            call complete_comm_batch%execute
            if (complete_comm_batch%stage == stage_final) then
                ! If batch has completed, remove it from the active batches list (requires a search)
                ib => active_batches%head
                do while (associated(ib))
                    select type (listBatch => ib%value)
                    type is (Batch)
                        if (listBatch%id == complete_comm_batch%id) then
                            call active_batches%remove(ib)
                            exit
                        end if
                        ib => ib%next
                    end select
                end do
                nactive = nactive - 1
                ndone = ndone + 1
            end if
        else
            ib => active_batches%head
            do while (associated(ib))
                select type (thisBatch =>ib%value)
                type is (Batch)
                    if (thisBatch%status /= stat_waiting) then
                        productive = .true.
                        call thisBatch%execute
                        if (thisBatch%stage == 2) then
                            call active_batches%remove(ib)
                            nactive = nactive - 1
                            ndone = ndone + 1
                            if (.not. associated(active_batches%head)) then
                                exit
                            end if
                        end if
                    end if
                    ib => ib%next
                end select
            end do

            if (.not. productive) then
                if (nactive < max_active_batches) then
                    nactive = nactive + 1
                    ncurrent = ncurrent + 1
                    call activate(ncurrent)
                end if
            end if
        end if
    end do

contains

    subroutine activate(n)
        integer, intent(in) :: n

        ! Add a new Batch and Comm to the respective lists
        call active_batches%append(Batch(n))
        call active_comms%append(Comm(n))

        select type (new_batch => active_batches%tail%value)
        type is (Batch)
            select type(new_comm => active_comms%tail%value)
            type is (Comm)
                ! Associate new Batch and Comm with each other
                new_comm%my_batch => new_batch
                new_batch%my_comm => new_comm
            end select
            ! Start communication or set as pending
            if (ncomm_started < max_comms) then
                ncomm_started = ncomm_started + 1
                !call start(new_comm)
                new_batch%status = stat_waiting
            else
                new_batch%status = stat_pending
            endif
        end select
        write(*,*) "batch/comm", n, "activated"
    end subroutine activate

    subroutine print_ids(node)
        type(LinkedListNode), pointer, intent(inout) :: node
        
        select type(p => node%value)
            class is(Batch)
                write(*,*) "Batch ID = ", p%id
            class is(Comm)
                write(*,*) "Got Comm"
        class default
            write(*,*) "ERROR"
        end select
    end subroutine print_ids

end program overlap_demonstrator
