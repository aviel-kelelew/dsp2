import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class help1_KeyForFirstJoin implements WritableComparable<help1_KeyForFirstJoin> {
    private String decade;
    private String word1;
    private String sign;

    public help1_KeyForFirstJoin() {
        decade = "0";
        word1 = "";
        sign = "";
    }

    public help1_KeyForFirstJoin(String decade, String word1, String sign) {
        this.decade = decade;
        this.word1 = word1;
        this.sign = sign;
    }

    @Override
    public int compareTo(help1_KeyForFirstJoin other) {
        int ret = (int) (decade.compareTo(other.getDecade()));
        if (ret == 0) {
            ret = (int) (word1.compareTo(other.getWord1()));
        }
        if (ret == 0) {
            ret = (int) (sign.compareTo(other.getSign()));
            }
        return ret;
    }

    public String getDecade() {
        return decade;
    }

    public String getWord1() {
        return word1;
    }

    public String getSign() {
        return sign;
    }

    public void setWord1(String word1) {
        this.word1 = word1;
    }

    public void setSign(String word2) {
        this.sign = word2;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(decade);
        dataOutput.writeUTF(word1);
        dataOutput.writeUTF(sign);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        decade = dataInput.readUTF();
        word1 = dataInput.readUTF();
        sign = dataInput.readUTF();
    }

    public String toString() {
        return this.decade + "\t" + this.word1;
    }

    public int tmphashCode() {
        return Integer.parseInt(decade);
    }
}
