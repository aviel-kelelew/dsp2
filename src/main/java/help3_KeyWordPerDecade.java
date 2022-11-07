import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class help3_KeyWordPerDecade implements WritableComparable<help3_KeyWordPerDecade> {
    private int decade;
    private String word1;  //we are using with big data . is int enough?  //count of w1w2
    private String word2;

    public help3_KeyWordPerDecade(){
        decade = -1;
        word1 = "";
        word2 = "";
    }

    public help3_KeyWordPerDecade(int year, String word1, String word2){
        this.decade = year/10;
        this.word1 = word1;
        this.word2 = word2;
    }

    @Override
    public int compareTo(help3_KeyWordPerDecade other) {
        int ret = (int)(decade -other.getDecade());
        if(ret ==0){
            ret = (int)(word1.compareTo(other.word1));
            if(ret==0){
                ret=(int)(word2.compareTo(other.word2));
            }
        }
        return ret;
    }

    public int getDecade(){
        return decade;
    }

    public String getWord1(){
        return word1;
    }

    public String getWord2(){
        return word2;
    }

    public void setWord1(String word1){
        this.word1 = word1;
    }

    public void setWord2(String word2){
        this.word2 = word2;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(decade);
        dataOutput.writeUTF(word1);
        dataOutput.writeUTF(word2);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        decade = dataInput.readInt();
        word1 = dataInput.readUTF();
        word2 = dataInput.readUTF();
    }

    public String toString(){
        return this.word1+"\t"+this.word2+"\t"+String.valueOf(this.decade);
    }

    public int LikehashCode() {
        return decade;
    }
}
