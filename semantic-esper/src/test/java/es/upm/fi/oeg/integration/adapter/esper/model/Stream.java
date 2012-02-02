package es.upm.fi.oeg.integration.adapter.esper.model;



public class Stream 
{
    public final static int SIZE = Double.SIZE;

private long time;//ms
private final long inTime;
private double windspeed;

public Stream(double windspeed) {
    this();
    this.windspeed = windspeed;

    }

private Stream()
{
    this.inTime = System.nanoTime();
    setTime(inTime);
}

private void setTime(long time) {
	this.time = time;
}

public long getTime() {
	return time;
}

public void setWindspeed(double windspeed) {
	this.windspeed = windspeed;
}

public double getWindspeed() {
	return windspeed;
}

public long getInTime() {
	return inTime;
}

public String toString() {
    return windspeed+" : "+time+" : ";
}

public Object clone() throws CloneNotSupportedException {
    return new Stream(windspeed);
}
}
