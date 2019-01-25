package com.example.learning.utils;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CompositeKey implements Writable, WritableComparable<CompositeKey> {
    Logger logger = Logger.getLogger(this.getClass());
    private String deptId;
    private String lastName;
    private String firstName;

    @Override
    public String toString() {
        return "CompositeKey{" +
                "deptId='" + deptId + '\'' +
                ", lastName='" + lastName + '\'' +
                ", firstName='" + firstName + '\'' +
                ", empId='" + empId + '\'' +
                '}';
    }

    private String empId;

    public CompositeKey() {
        this.deptId = "";
        this.lastName = "";
        this.firstName = "";
        this.empId = "";
    }

    public CompositeKey(String deptId, String lastName, String firstName, String empId) {
        this.deptId = deptId;
        this.lastName = lastName;
        this.firstName = firstName;
        this.empId = empId;
    }

    @Override
    public int compareTo(CompositeKey compositeKey) {
        logger.info("Entring compareTo method");
        try {
            if (compositeKey != null && compositeKey.getDeptId() != null && this.deptId.compareTo(compositeKey.getDeptId()) != 0) {
                return this.deptId.compareTo(compositeKey.getDeptId());
            } else if (compositeKey != null && compositeKey.getLastName() != null && this.lastName.compareTo(compositeKey.getLastName()) != 0) {
                return this.lastName.compareTo(compositeKey.getLastName());
            } else if (compositeKey != null && compositeKey.getFirstName() != null && this.firstName.compareTo(compositeKey.getFirstName()) != 0) {
                return this.firstName.compareTo(compositeKey.getFirstName());
            } else if (compositeKey != null && compositeKey.getEmpId() != null && this.empId.compareTo(compositeKey.getEmpId()) != 0) {
                return this.empId.compareTo(compositeKey.getEmpId());
            } else {
                return 0;
            }
        } finally {
            logger.info("Exiting compareTo method");
        }
    }

    public String getDeptId() {
        return deptId;
    }

    public void setDeptId(String deptId) {
        this.deptId = deptId;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getEmpId() {
        return empId;
    }

    public void setEmpId(String empId) {
        this.empId = empId;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.getDeptId());
        dataOutput.writeUTF(this.getLastName());
        dataOutput.writeUTF(this.getFirstName());
        dataOutput.writeUTF(this.getEmpId());

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.deptId = dataInput.readUTF();
        this.lastName = dataInput.readUTF();
        this.firstName = dataInput.readUTF();
        this.empId = dataInput.readUTF();
    }
}
