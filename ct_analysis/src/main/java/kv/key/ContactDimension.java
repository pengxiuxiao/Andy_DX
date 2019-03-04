package kv.key;

import kv.base.BaseDimension;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * @Author: pxx
 * @Date: 2019/3/4 21:27
 * @Version 1.0
 */
@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class ContactDimension extends BaseDimension {
    private String telephopne;
    private String name;

    @Override
    public int compareTo(BaseDimension o) {
        ContactDimension anotherContactDimendion = (ContactDimension) o;
        int result = this.name.compareTo(anotherContactDimendion.name);
        if (result != 0) {
            return result;
        }
        result  = this.telephopne.compareTo(anotherContactDimendion.telephopne);
        return result;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.telephopne);
        out.writeUTF(this.name);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.telephopne = in.readUTF();
        this.name = in.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ContactDimension that = (ContactDimension) o;
        return Objects.equals(telephopne, that.telephopne) &&
                Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(telephopne, name);
    }
}
