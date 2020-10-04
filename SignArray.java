import java.util.*;

public class SignArray {
    public static void main(String[] args)
    {
        Scanner sc = new Scanner(System.in);
        int n = sc.nextInt();
        int arr[] = new int[n];
        for (int i = 0; i < n; i++) {
            arr[i] = sc.nextInt();
        }
        partitionNegativeAndPositive(n, arr);
    }


    static void printArray(ArrayList<Integer> list)
    {
        String output="";
        for(int i=0;i<list.size();i++) {
            output=output+list.get(i)+" ";
        }
        System.out.println(output);
    }

    // Method to partition negative and positive numbers without comparing with 0
    static void partitionNegativeAndPositive(int n, int arr[])
    {
        // Write your code here
        ArrayList<Integer> a = new ArrayList<Integer>();
        ArrayList<Integer> b = new ArrayList<Integer>();
        String firstNum;

        //Check if the first number is Negative/Positive
        if((1 + (arr[0] >> 31) - (-arr[0] >> 31))+(1 + (arr[0] >> 31) - (-arr[0] >> 31))==(1 + (arr[0] >> 31) - (-arr[0] >> 31)))
        {
            firstNum="Negative";
        }
        else
        {
            firstNum="Positive";
        }

//Running the same logic for all the numbers and categorising them to different arrays using below logic
//Output of the shift operation would give 0 for Neg, 1 for Zero and 2 for Positive
//0+0=0; 1+1=2; 2+2=4;
        for (int i = 0; i < n; i++) {
            if((1 + (arr[i] >> 31) - (-arr[i] >> 31))+(1 + (arr[i] >> 31) - (-arr[i] >> 31))==(1 + (arr[i] >> 31) - (-arr[i] >> 31)))
            {
                b.add(arr[i]);// For Negatives
            }
            else
            {
                a.add(arr[i]);// For Positives and Zeros
            }
        }

//Calling the print function based on the sign of the first number in the array
        if(firstNum=="Positive" )
        {
            printArray(a);
            if(b.size()!=0)
            {
            printArray(b);}
        }else {
            printArray(b);
            if(a.size()!=0)
            {
            printArray(a);}
        }
        if(a.size()==0) {
            System.out.println("Array doesn't have positive numbers");
        }
        else if(b.size()==0) {
            System.out.println("Array doesn't have negative numbers");
        }
    }
}