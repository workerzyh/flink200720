package bean;

/**
 * @author zhengyonghong
 * @create 2020--12--11--10:46
 */
public class SensorReading {
    private String id;
    private Long ts;
    private Double temp;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    public Double getTemp() {
        return temp;
    }

    public void setTemp(Double temp) {
        this.temp = temp;
    }

    public SensorReading() {
    }

    public SensorReading(String id, Long ts, Double temp) {
        this.id = id;
        this.ts = ts;
        this.temp = temp;
    }

    @Override
    public String toString() {
        return "SensorReading{" +
                "id='" + id + '\'' +
                ", ts=" + ts +
                ", temp=" + temp +
                '}';
    }
}
