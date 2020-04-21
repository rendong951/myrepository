package com.cictec.bigdata.busanalyse;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PointBean implements Writable {
    private String lineId;
    private String ICId;
    private String boardingTime;
    private String leavingTime;
    private String boardingStation;
    private String leavingStation;
    private int transfer;
    private String totalTime;

    public void set(String lineId, String ICId, String boardingTime, String leavingTime, String boardingStation, String leavingStation, int transfer, String totalTime) {
        this.lineId = lineId;
        this.ICId = ICId;
        this.boardingStation = boardingStation;
        this.leavingStation = leavingStation;
        this.boardingTime = boardingTime;
        this.leavingTime = leavingTime;
        this.transfer = transfer;
        this.totalTime = totalTime;
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

    public int getTransfer() {
        return transfer;
    }

    public String getTotalTime() {
        return totalTime;
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

    public void setTransfer(int transfer) {
        this.transfer = transfer;
    }

    public void setTotalTime(String totalTime) {
        this.totalTime = totalTime;
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        this.lineId = in.readUTF();
        this.ICId = in.readUTF();
        this.boardingTime = in.readUTF();
        this.leavingTime = in.readUTF();
        this.boardingStation = in.readUTF();
        this.leavingStation = in.readUTF();
        this.transfer = in.readInt();
        this.totalTime = in.readUTF();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(lineId);
        out.writeUTF(ICId);
        out.writeUTF(boardingTime);
        out.writeUTF(leavingTime);
        out.writeUTF(boardingStation);
        out.writeUTF(leavingStation);
        out.writeInt(transfer);
        out.writeUTF(totalTime);
    }

    @Override
    public String toString() {
        return lineId + "\001" + ICId + "\001" + boardingTime + "\001" + leavingTime + "\001" +
                boardingStation + "\001" + leavingStation + "\001" + transfer + "\001" + totalTime;
    }
}
