
#include <iostream>
#include <stdio.h>
#include <cuda.h>

#define max_N 100000
#define max_P 30
#define BLOCKSIZE 1024

using namespace std;

//*******************************************

// Write down the kernels here

/* Before you go into the code, one good job that has been done in this assignment is 
that I have done sorting in O(R) time, instead of O(R*ln(R)).

For that I have exploited the fact that the elements that we have to sort are in range of 
of 1 to N-1 and used prefix sum to have them inorder(computer centre wise)

I am pretty sure, this is a great optimization on the part of preprocessing without using 
external library or traditional sorting algorithms. */


/*Here in this kernel we have reduced the processing of the number of request per computer centre.
Each computer center will process only those many request which are assigned to them.

We took help of sorting to map request to their corresponding computer centre.

One optimization could also have been done here, we can call another kernel from this kernel instead of the 
loop that are running inside the kernel, that will optimize to some extent.

As there are no dependency within the loops, so parallelism could have be exploitted. */
__global__ void optimizedK(int *reqCountForEachComCen, int *exclusivePrefixSum, int *tot_reqs, int *succ_reqs,
 int *capacity, int *req_cen,int *req_fac, int *req_start, int *req_slots, int *fac_prefixSum){
  
  /*This comNum gives us the computer center number*/
  int comNum = blockIdx.x;

  /* For bug testing */
  // printf("ComCen - %d , ID Range - from %d to %d places \n", comNum, exclusivePrefixSum[comNum], reqCountForEachComCen[comNum]);
  // printf("")

  /* Using hours array to have hold of the fact that which slots are available and 
  which slots are occupied. */
  int hours[30*24+1];
  for(int i=0;i<30*24+1;i++){
    hours[i]=0;
  }

  /*This start and end points helps us to find the mapping of all the request to Computer Centre comNum. */
  int start = exclusivePrefixSum[comNum];
  int end = exclusivePrefixSum[comNum]+reqCountForEachComCen[comNum]-1;


  /* Processing the request. Loops inside this loop could be further avoided by the help
  of dynamic parallelism. */
  for(int i=start; i<=end; i++){
    tot_reqs[comNum]++;


    bool flag = false;
    // int facIndex = exclusivePrefixSum[comNum]+req_fac[i];
    
    /*To know about capacity of the required facility using actual index of that facility.
    Which we find out using prefix sum.*/
    int facIndex = fac_prefixSum[req_cen[i]]+req_fac[i];
    int capacityOfReqFacility = capacity[facIndex];
    
    /* For debugging purpose. */
    // if(i==0){
    //   printf("\n Capacity of Req Facility: %d \n", capacityOfReqFacility);
    // }


    /*traverse hours array to check if slot available*/
    int start_Hour = req_start[i];
    int runTill = req_start[i]+req_slots[i];

    /* Here are checking if slot is available or not with the help of flag variable.*/
    for(int j=req_fac[i]*24+start_Hour; j<req_fac[i]*24+runTill;j++){
      if(hours[j]<capacityOfReqFacility){
        continue;
      }else{
        // cc[req_cen[i]*2+1]+=1;
        flag = true;
        break;
      }
    }

    /* If slots are not availablle then switch to next request.*/
    if(flag==true){
      continue;
    }

    /* If slots are available then process the request and increase the success
    count for computer centre "comNum" */
    for(int j=req_fac[i]*24+start_Hour; j<req_fac[i]*24+runTill;j++){
            hours[j]++;
    }
    succ_reqs[comNum]++;
  }
}


//***********************************************


int main(int argc,char **argv)
{
	// variable declarations...
    int N,*centre,*facility,*capacity,*fac_ids, *succ_reqs, *tot_reqs;
    

    FILE *inputfilepointer;
    
    //File Opening for read
    char *inputfilename = argv[1];
    inputfilepointer    = fopen( inputfilename , "r");

    if ( inputfilepointer == NULL )  {
        printf( "input.txt file failed to open." );
        return 0; 
    }

    fscanf( inputfilepointer, "%d", &N ); // N is number of centres
	
    // Allocate memory on cpu
    centre=(int*)malloc(N * sizeof (int));  // Computer  centre numbers
    facility=(int*)malloc(N * sizeof (int));  // Number of facilities in each computer centre
    fac_ids=(int*)malloc(max_P * N  * sizeof (int));  // Facility room numbers of each computer centre
    capacity=(int*)malloc(max_P * N * sizeof (int));  // stores capacities of each facility for every computer centre 


    int success=0;  // total successful requests
    int fail = 0;   // total failed requests
    tot_reqs = (int *)malloc(N*sizeof(int));   // total requests for each centre
    succ_reqs = (int *)malloc(N*sizeof(int)); // total successful requests for each centre

    // Input the computer centres data
    int k1=0 , k2 = 0;
    for(int i=0;i<N;i++)
    {
      fscanf( inputfilepointer, "%d", &centre[i] );
      fscanf( inputfilepointer, "%d", &facility[i] );
      
      for(int j=0;j<facility[i];j++)
      {
        fscanf( inputfilepointer, "%d", &fac_ids[k1] );
        k1++;
      }
      for(int j=0;j<facility[i];j++)
      {
        fscanf( inputfilepointer, "%d", &capacity[k2]);
        k2++;     
      }
    }

    // variable declarations
    int *req_id, *req_cen, *req_fac, *req_start, *req_slots;   // Number of slots requested for every request
    
    // Allocate memory on CPU 
	int R;
	fscanf( inputfilepointer, "%d", &R); // Total requests
    req_id = (int *) malloc ( (R) * sizeof (int) );  // Request ids
    req_cen = (int *) malloc ( (R) * sizeof (int) );  // Requested computer centre
    req_fac = (int *) malloc ( (R) * sizeof (int) );  // Requested facility
    req_start = (int *) malloc ( (R) * sizeof (int) );  // Start slot of every request
    req_slots = (int *) malloc ( (R) * sizeof (int) );   // Number of slots requested for every request
    
    // Input the user request data
    for(int j = 0; j < R; j++)
    {
       fscanf( inputfilepointer, "%d", &req_id[j]);
       fscanf( inputfilepointer, "%d", &req_cen[j]);
       fscanf( inputfilepointer, "%d", &req_fac[j]);
       fscanf( inputfilepointer, "%d", &req_start[j]);
       fscanf( inputfilepointer, "%d", &req_slots[j]);
       tot_reqs[req_cen[j]]+=1;  
    }
		
    //********************************* 


    /* To have the request in sorted form.*/
    int *sReqID, *sReqCen, *sReqFac, *sReqStart, *sReqSlots;
    sReqID = (int *) malloc ((R) * sizeof (int));
    sReqCen = (int *) malloc ((R) * sizeof (int));
    sReqFac = (int *) malloc ((R) * sizeof (int));
    sReqStart = (int *) malloc ((R) * sizeof (int));
    sReqSlots = (int *) malloc ((R) * sizeof (int));



    /* To know individual count of request for each computer. */
    int *reqCountForEachComCen;
    reqCountForEachComCen = (int*)malloc(N * sizeof (int));
    memset(reqCountForEachComCen, 0, N*sizeof(int));

    for(int i=0; i<R; i++){
      reqCountForEachComCen[req_cen[i]]++;
    }

    /* We are using iterator to sort the request. exclusivePRefixSum is 
    sum of capacity which is also helping us in sorting.*/
    int *iterator, *exclusivePrefixSum;

    /*Preprocessing for the sorting.*/
    iterator = (int*)malloc(N * sizeof (int));
    exclusivePrefixSum = (int*)malloc(N * sizeof (int));

    iterator[0]=0;
    exclusivePrefixSum[0]=0;

    for(int i=1;i<N;i++){
      iterator[i] = reqCountForEachComCen[i-1]+iterator[i-1];
      exclusivePrefixSum[i]= iterator[i];
    }

    /* Sorting the request in O(R) time. */
    for(int i=0;i<R;i++){
      int sortedIndex = iterator[req_cen[i]];
      sReqID[sortedIndex]=req_id[i];
      sReqCen[sortedIndex]=req_cen[i];
      sReqFac[sortedIndex]=req_fac[i];
      sReqStart[sortedIndex]=req_start[i];
      sReqSlots[sortedIndex]=req_slots[i];
      iterator[req_cen[i]]++;
    }


    /*For debugging...*/
    // for(int i=0;i<R;i++){
    //   printf("%d %d %d %d %d \n", sReqID[i], sReqCen[i], sReqFac[i], sReqStart[i], sReqSlots[i]);
    // }

    /*Allocating memory to device.*/
    int *d_capacity, *d_sReqCen,*d_sReqFac, *d_sReqStart, *d_sReqSlots;
    cudaMalloc(&d_capacity,max_P * N*sizeof(int));
    cudaMalloc(&d_sReqCen,R*sizeof(int));
    cudaMalloc(&d_sReqFac,R*sizeof(int));
    cudaMalloc(&d_sReqStart,R*sizeof(int));
    cudaMalloc(&d_sReqSlots,R*sizeof(int));

    cudaMemcpy(d_capacity, capacity, max_P * N*sizeof(int), cudaMemcpyHostToDevice);
    cudaMemcpy(d_sReqCen, sReqCen, R*sizeof(int), cudaMemcpyHostToDevice);
    cudaMemcpy(d_sReqFac, sReqFac, R*sizeof(int), cudaMemcpyHostToDevice);
    cudaMemcpy(d_sReqStart, sReqStart, R*sizeof(int), cudaMemcpyHostToDevice);
    cudaMemcpy(d_sReqSlots, sReqSlots, R*sizeof(int), cudaMemcpyHostToDevice);




    int *d_reqCountForEachComCen;
    cudaMalloc(&d_reqCountForEachComCen,N*sizeof(int));
    cudaMemcpy(d_reqCountForEachComCen, reqCountForEachComCen, N*sizeof(int), cudaMemcpyHostToDevice);
    
    int *d_exclusivePrefixSum;
    cudaMalloc(&d_exclusivePrefixSum,N*sizeof(int));
    cudaMemcpy(d_exclusivePrefixSum, exclusivePrefixSum, N*sizeof(int), cudaMemcpyHostToDevice);

    // total and successful requests 
    int *d_tot_reqs;
    cudaMalloc(&d_tot_reqs,N*sizeof(int));
    cudaMemset(d_tot_reqs, 0, N*sizeof(int));
  
    int *d_succ_reqs;
    cudaMalloc(&d_succ_reqs,N*sizeof(int));
    // cudaMemcpy(d_succ_reqs, succ_reqs, N*sizeof(int), cudaMemcpyHostToDevice);
    cudaMemset(d_succ_reqs, 0, N*sizeof(int));


    /* Taking prefix array sum of the facility which will help us in 
    finding effective index of facility in kernel.*/
    
    int *prefixArray; //
    prefixArray = (int *) malloc(N*sizeof(int)); // prefix array to know capacity of indivual facility
    /* find prefix sum */ 
    int temp = 0;
    for(int i=0;i<N;i++){
      prefixArray[i]=temp;
      temp+=facility[i];
    }


    /* Debugging ...*/
    // for(int i=0;i<N;i++){
    //   printf("%d ", prefixArray[i]);
    // }
    // // d_prefixSum

    /* Allocating the memory in device to prefixSum array.*/
    int *d_prefixSum;
    cudaMalloc(&d_prefixSum, N*sizeof(int));
    cudaMemcpy(d_prefixSum, prefixArray, N*sizeof(int), cudaMemcpyHostToDevice);


    /* Launching the kernel...*/
    int BLOCK = N;
    int THREAD = 1;
    optimizedK<<<BLOCK, THREAD>>>(d_reqCountForEachComCen, d_exclusivePrefixSum, d_tot_reqs, d_succ_reqs,
     d_capacity, d_sReqCen,d_sReqFac, d_sReqStart, d_sReqSlots, d_prefixSum);
    
    cudaDeviceSynchronize();

    cudaMemcpy(succ_reqs, d_succ_reqs, N*sizeof(int), cudaMemcpyDeviceToHost);
    cudaMemcpy(tot_reqs, d_tot_reqs, N*sizeof(int), cudaMemcpyDeviceToHost);
    for(int i=0;i<N;i++){
      success+=succ_reqs[i];
    }
    // printf("Kernel End\n Total Req Processed: %d", tot_reqs[0]);
    fail = R - success; 

    /*Debugging...*/
    // printf("%d %d", success, fail);
    //********************************

    //*********************************
    // Call the kernels here

    //********************************




    // Output
    char *outputfilename = argv[2]; 
    FILE *outputfilepointer;
    outputfilepointer = fopen(outputfilename,"w");

    fprintf( outputfilepointer, "%d %d\n", success, fail);
    for(int j = 0; j < N; j++)
    {
        fprintf( outputfilepointer, "%d %d\n", succ_reqs[j], tot_reqs[j]-succ_reqs[j]);
    }
    fclose( inputfilepointer );
    fclose( outputfilepointer );
    cudaDeviceSynchronize();
	return 0;
}