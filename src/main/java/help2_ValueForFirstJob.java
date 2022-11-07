import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class help2_ValueForFirstJob implements WritableComparable<help2_ValueForFirstJob> {
    private long numberWordInDecade;
    private long numerOfOcc;
    private long numOfOccOfW1W2;


    public help2_ValueForFirstJob(){
        numberWordInDecade = -1;
        numerOfOcc = -1;
        numOfOccOfW1W2 = -1;
    }

    public help2_ValueForFirstJob(long numberWordInDecade, long numerOfOcc){
      this.numberWordInDecade = numberWordInDecade;
      this.numerOfOcc = numerOfOcc;
      this.numOfOccOfW1W2 = 1;
    }

    public help2_ValueForFirstJob(long numberWordInDecade, long numerOfOcc, long numOfOccOfW1W2){
        this.numberWordInDecade = numberWordInDecade;
        this.numerOfOcc = numerOfOcc;
        this.numOfOccOfW1W2 =numOfOccOfW1W2;
    }

    public help2_ValueForFirstJob(long numOfOccOfW1W2){
        this.numberWordInDecade =0;
        this.numerOfOcc = 0;
        this.numOfOccOfW1W2 = numOfOccOfW1W2;
    }

    @Override
    public int compareTo(help2_ValueForFirstJob other) {
        int ret = 0;
        long check= 0;
        check = numberWordInDecade - other.getNumberWordInDecade();
        if(check ==0){
            check = numerOfOcc -other.getNumerOfOcc();
           if(check == 0){
               check = numOfOccOfW1W2- other.getNumOfOccOfW1W2();
           }
        }
        ret = (int)check;
        return ret;
    }

    public long getNumberWordInDecade(){
        return numberWordInDecade;
    }

    public long getNumerOfOcc(){
        return numerOfOcc;
    }

    public long getNumOfOccOfW1W2(){
        return numOfOccOfW1W2;
    }
    public void setNumberWordInDecade(int NumberWordInDecade){
        this.numberWordInDecade = NumberWordInDecade;
    }

    public void setNumerOfOcc(int NumerOfOcc){
        this.numerOfOcc = NumerOfOcc;
    }

    public void setNumOfOccOfW1W2(int numOfOccOfW1W2){
        this.numOfOccOfW1W2 = numOfOccOfW1W2;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(numberWordInDecade);
        dataOutput.writeLong(numerOfOcc);
        dataOutput.writeLong(numOfOccOfW1W2);


    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        numberWordInDecade = dataInput.readLong();
        numerOfOcc = dataInput.readLong();
        numOfOccOfW1W2 = dataInput.readLong();
    }

    public String toString(){
        return this.numberWordInDecade+"\t"+this.numerOfOcc+"\t"+this.numOfOccOfW1W2;
    }
}
