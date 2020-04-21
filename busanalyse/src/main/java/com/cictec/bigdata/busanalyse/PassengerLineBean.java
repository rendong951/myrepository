package com.cictec.bigdata.busanalyse;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PassengerLineBean implements Writable {
    private String lineId;
    private String ICId;
    private String boardingTime;
    private String leavingTime;
    private String boardingStation;
    private String leavingStation;
    private String busLine;
    private int step;

    public void set(String lineId, String ICId, String boardingTime, String leavingTime, String boardingStation, String leavingStation, String busLine, int step) {
        this.lineId = lineId;
        this.ICId = ICId;
        this.boardingTime = boardingTime;
        this.leavingTime = leavingTime;
        this.boardingStation = boardingStation;
        this.leavingStation = leavingStation;
        this.busLine = busLine;
        this.step = step;
    }

    public String getLineId() {
        return lineId;
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

    public String getBusLine() {
        return busLine;
    }

    public int getStep() {
        return step;
    }

    public void setLineId(String lineId) {
        this.lineId = lineId;
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

    public void setBusLine(String busLine) {
        this.busLine = busLine;
    }

    public void setStep(int step) {
        this.step = step;
    }

    @Override
    public String toString() {
        return lineId + "\001" + ICId + "\001" + boardingTime + "\001" + leavingTime + "\001" +
                boardingStation + "\001" + leavingStation + "\001" + busLine + "\001" + step;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.lineId = in.readUTF();
        this.ICId = in.readUTF();
        this.boardingTime = in.readUTF();
        this.leavingTime = in.readUTF();
        this.boardingStation = in.readUTF();
        this.leavingStation = in.readUTF();
        this.busLine = in.readUTF();
        this.step= in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(lineId);
        out.writeUTF(ICId);
        out.writeUTF(boardingTime);
        out.writeUTF(leavingTime);
        out.writeUTF(boardingStation);
        out.writeUTF(leavingStation);
        out.writeUTF(busLine);
        out.writeInt(step);
    }
}
