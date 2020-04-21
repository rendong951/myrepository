package com.cictec.bigdata.busanalyse;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SourceDataBean implements Writable {
    private String ICId;
    private String boardingTime;
    private String leavingTime;
    private String boardingStation;
    private String leavingStation;
    private String line;

    public void set(String ICId, String boardingTime, String leavingTime, String boardingStation, String leavingStation, String line) {
        this.ICId = ICId;
        this.boardingStation = boardingStation;
        this.leavingStation = leavingStation;
        this.boardingTime = boardingTime;
        this.leavingTime = leavingTime;
        this.line = line;
    }

    public void setICId(String ICId) {
        this.ICId = ICId;
    }

    public void setBoardingTime(String boardingTime) {
        this.boardingTime = boardingTime;
    }

    public void setLeavingTime(String leavingTime) {
        this.leavingTime = leavingTime;
    }

    public void setBoardingStation(String boardingStation) {
        this.boardingStation = boardingStation;
    }

    public void setLeavingStation(String leavingStation) {
        this.leavingStation = leavingStation;
    }

    public void setLine(String line) {
        this.line = line;
    }

    public String getICId() {
        return ICId;
    }

    public String getBoardingTime() {
        return boardingTime;
    }

    public String getLeavingTime() {
        return leavingTime;
    }

    public String getBoardingStation() {
        return boardingStation;
    }

    public String getLeavingStation() {
        return leavingStation;
    }

    public String getLine() {
        return line;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.ICId = in.readUTF();
        this.boardingTime = in.readUTF();
        this.leavingTime = in.readUTF();
        this.boardingStation = in.readUTF();
        this.leavingStation = in.readUTF();
        this.line = in.readUTF();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(ICId);
        out.writeUTF(boardingTime);
        out.writeUTF(leavingTime);
        out.writeUTF(boardingStation);
        out.writeUTF(leavingStation);
        out.writeUTF(line);
    }
}
