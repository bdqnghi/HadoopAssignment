package com.hadoop.assignment;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by quocnghi on 11/10/16.
 */
public class UserLocationKey implements WritableComparable<UserLocationKey> {

    private String locationId;
    private String userId;

    public UserLocationKey(String userId, String locationId) {
        this.locationId = locationId;
        this.userId = userId;
    }

    public UserLocationKey() {

    }

    public int compareTo(UserLocationKey o) {
        int result = userId.compareTo(o.userId);
        if(0 == result) {
            result = locationId.compareTo(o.locationId);
        }
        return result;
    }

    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeString(dataOutput, userId);
        WritableUtils.writeString(dataOutput, locationId);
        //dataOutput.writeLong(locationId);
    }

    public void readFields(DataInput dataInput) throws IOException {
        userId = WritableUtils.readString(dataInput);
        //locationId = dataInput.readLong();
        locationId = WritableUtils.readString(dataInput);
    }

    @Override
    public String toString() {

        return userId.toString() + "," + locationId.toString();
    }

    public String getUserId(){
        return userId;
    }

    public String getLocationId(){
        return locationId;
    }
}
