program overlap_demonstrator
    use linked_list_m  , only: LinkedList, LinkedListNode
    use overlap_types_mod !, only: Batch, Comm, stat_waiting, stat_pending, CommList, BatchList
    use common_data
    use common_mpi
!    use mpi
    
    implicit none


    logical :: comm_compl
    logical :: productive

    integer :: ndone
    integer :: ncurrent,ierr,i
    type(LinkedListNode), pointer :: ic
    type(LinkedListNode), pointer :: ib,next
    type(Batch), pointer :: complete_comm_batch
    type(comm), pointer :: c
    
    ! Test everything works as expected
 !   write(*,*) "Testing BatchList/CommList functionality"

    ! Activate three batches/comms
 !   write(*,*) "Call activate 3 times"
 !   call activate(1)
 !   call activate(2)
 !   call activate(3)

    ! Print IDs of all active batches
 !   write(*,*) "Print all batch IDs"
 !   call active_batches%traverse(print_ids)

    ! Remove middle batch
 !   write(*,*) "Remove middle batch"
 !   ib => active_batches%head%next
 !   call active_batches%remove(ib)

    ! Print IDs again
 !   write(*,*) "Print all batch IDs again"
 !   call active_batches%traverse(print_ids)

    ! Remove all batches
 !   write(*,*) "Reset"
 !   call active_batches%reset
 !   call active_comms%reset

    ! Now start the actual work
    write(*,*)
    write(*,*) "Now start the actual work"

    call mpi_init(ierr)
    call mpi_comm_rank(mpi_comm_world,mytask,ierr)
    call mpi_comm_size(mpi_comm_world,ntasks,ierr)

    numsend = 100000
    numrecv = 100000
    
    allocate(sendbuf1(numsend*ntasks,nbatches))
    allocate(sendbuf2(numsend*ntasks,nbatches))
    allocate(recvbuf(numrecv*ntasks,nbatches))
    allocate(send_reqs(ntasks,nbatches))
    allocate(recv_reqs(ntasks,nbatches))
    allocate(off(ntasks))

    do i=1,ntasks
       off(i) = (i-1) * numsend
    enddo
    
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
                    call thisComm%finish
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
                       print *,'Starting comm for pending branch ',thisBatch%id
                       c => thisBatch%my_comm
                       print *,'Comm ',c%id
                       call thisBatch%my_comm%start(thisBatch%stage)
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

               ! This doesn't work:
!               call active_batches%remove(complete_comm_batch)
               
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
                   if (thisBatch%status /= stat_waiting .and.  &
                           thisBatch%status /= stat_pending) then
                        productive = .true.
                        call thisBatch%execute
                        if (thisBatch%stage == stage_final) then
                           next = ib%next
                           call active_batches%remove(ib)
                           nactive = nactive - 1
                           ndone = ndone + 1
                           if (.not. associated(active_batches%head)) then
                              exit
                           end if
                        else
                           next = ib%next
                        end if
                        ib = next
                     else
                        ib => ib%next
                     end if
                end select
            end do

            if (.not. productive .and. nactive < max_active_batches .and. ncurrent < nbatches) then
               nactive = nactive + 1
               ncurrent = ncurrent + 1
               call activate(ncurrent)
            end if
         end if
    end do

    print *,'Process ',mytask,'Completed'
    call mpi_finalize(ierr)
    
contains

    subroutine activate(n)
        integer, intent(in) :: n
        class(Batch), pointer :: new_batch,b
        class(Comm), pointer :: new_comm,c
        
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
                b => new_batch
                c => new_comm
                ! Start communication or set as pending
                if (ncomm_started < max_comms) then
                   ncomm_started = ncomm_started + 1
                   call new_comm%start(new_batch%stage)
                   new_batch%status = stat_waiting
                else
                   print *,'Batch ',new_batch%id, ', stage 1 is pending'
                   new_batch%status = stat_pending
                endif
             end select
             if (ncomm_started >= max_comms) then
                c => new_batch%my_comm
                print *,'Comm ',c%id
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
