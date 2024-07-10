FC = mpif90
FOPT = -g -O0
#-qopenmp -traceback -L/usr/local/apps/intel/2021.4.0/mpi/latest/lib -g

overlap_demonstrator: overlap_demonstrator.o overlap_types_mod.o linked_list_mod.o common_mpi.o common_data.o
	$(FC) $(FOPT) $^ -o overlap_demonstrator

overlap_demonstrator.o: overlap_demonstrator.f90 overlap_types_mod.o linked_list_mod.o common_mpi.o common_data.o
overlap_types_mod.o: overlap_types_mod.f90 linked_list_mod.o common_mpi.o common_data.o
# linked_list_mod.o: linked_list_mod.f90

%.o: %.f90
	$(FC) $(FOPT) -c $< -o $@

.PHONY: clean
clean:
	rm -f *.o *.mod overlap_demonstrator
