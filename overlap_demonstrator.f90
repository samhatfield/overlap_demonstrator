program overlap_demonstrator
    use linked_list_m  , only: LinkedList, LinkedListNode
    use overlap_types_mod !, only: Batch, Comm, stat_waiting, stat_pending, CommList, BatchList
    use common_data, only: nbatches, max_comms, max_active_batches, stage_final, stat_waiting, &
        & stat_pending, stat_exec, ncomm_started, nactive
    use common_mpi, only: ntasks, mytask, numsend, numrecv, sendbuf1, sendbuf2, recvbuf, off, &
        & recv_reqs, send_reqs
    use mpi, only: mpi_init, mpi_comm_rank, mpi_comm_world, mpi_comm_size, mpi_finalize
    
    implicit none


    logical :: comm_compl
    logical :: productive

    integer :: ndone
    integer :: ncurrent,ierr,i
    type(LinkedListNode), pointer :: ic
    type(LinkedListNode), pointer :: ib,next,ib1
    type(Batch), pointer :: complete_comm_batch
    
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
        ib => active_batches%head
        do while (associated(ib))
            select type (thisBatch => ib%value)
            type is (Batch)
               if(thisBatch%status .eq. stat_waiting) then
                  if (thisBatch%comm_complete()) then
                     ! complete_comm(ic%value) step needed here?
                     write(*,*) "Comm", thisBatch%id, " complete"
                     complete_comm_batch => thisBatch
                     call thisBatch%finish_comm
                     thisBatch%status = stat_exec
                     comm_compl = .true.
                     ncomm_started = ncomm_started - 1
                     exit
                  end if
               endif
                ib => ib%next
            end select
        end do
        
        if (comm_compl) then
            ! Now that one comm has completed, we can start the comm on the next batch whose comm is
            ! pending, if there is one
            ib1 => active_batches%head
            do while (associated(ib1))
                select type (thisBatch => ib1%value)
                type is (Batch)
                    if (thisBatch%status == stat_pending) then
                       print *,'Starting comm for pending branch ',thisBatch%id
                       call thisBatch%start_comm(thisBatch%stage)
                       thisBatch%status = stat_waiting
                       ncomm_started = ncomm_started + 1
                        exit
                    end if
                    ib1 => ib1%next
                end select
            end do

            productive = .true.
            call complete_comm_batch%execute
            if (complete_comm_batch%stage == stage_final) then
               ! If batch has completed, remove it from the active batches list (requires a search)

               ! This doesn't work:
!               call active_batches%remove(complete_comm_batch)
               
!               ib => active_batches%head
!                do while (associated(ib))
!                    select type (listBatch => ib%value)
!                    type is (Batch)
!                        if (listBatch%id == complete_comm_batch%id) then
                            call active_batches%remove(ib)
!                            exit
!                        end if
!                        ib => ib%next
!                    end select
!                end do
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
        
        ! Add a new Batch and Comm to the respective lists
        call active_batches%append(Batch(n))

        select type (new_batch => active_batches%tail%value)
        type is (Batch)
           b => new_batch
           ! Start communication or set as pending
           if (ncomm_started < max_comms) then
              ncomm_started = ncomm_started + 1
              call new_batch%start_comm(new_batch%stage)
              new_batch%status = stat_waiting
           else
              print *,'Batch ',new_batch%id, ', stage 1 is pending'
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
        class default
            write(*,*) "ERROR"
        end select
    end subroutine print_ids

end program overlap_demonstrator
