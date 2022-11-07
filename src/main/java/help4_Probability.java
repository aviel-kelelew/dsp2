import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class help4_Probability implements WritableComparable<help4_Probability> {
    private int decade;
    private String w1;
    private String w2;
    private double probability;

    public help4_Probability() {
        this.decade = 0;
        this.w1 = "";
        this.w2 = "";
        probability = 0;
    }

    public help4_Probability(int decade, String word1, String word2, double probability) {
        this.decade = decade;
        this.w1 = word1;
        this.w2 = word2;
        this.probability = probability;
    }

    @Override
    public int compareTo(help4_Probability other) {
        int ret = (int) (decade - other.getDecade());
        if (ret == 0) {
           if(probability == other.getProbability()){return 0;}
           else if(probability > other.getProbability()) {return -1;}
           else{return 1;}
        }
        return ret;
    }

    public int getDecade() {
        return decade;
    }

    public String getWord1() {
        return w1;
    }

    public String getWord2() {
        return w2;
    }

    public double getProbability(){
        return probability;
    }

    public void setWord1(String word1) {
        this.w1 = word1;
    }

    public void setWord2(String word2) {
        this.w2 = word2;
    }
    public void setProbability(double probability){
        this.probability = probability;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(decade);
        dataOutput.writeUTF(w1);
        dataOutput.writeUTF(w2);
        dataOutput.writeDouble(probability);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        decade = dataInput.readInt();
        w1 = dataInput.readUTF();
        w2 = dataInput.readUTF();
        probability= dataInput.readDouble();
    }

    public String toString() {
        return this.decade + "\t" + this.w1+"\t" + this.w2+"\t" + this.probability;
    }

    public int ghashCode() {
        return decade;
    }
}
