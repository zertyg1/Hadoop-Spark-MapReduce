package org.example;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MovieRatingWritable implements Writable {
    private String movieId;
    private double rating;

    public MovieRatingWritable() {}

    public MovieRatingWritable(String movieId, double rating) {
        this.movieId = movieId;
        this.rating = rating;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(movieId);
        out.writeDouble(rating);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        movieId = in.readUTF();
        rating = in.readDouble();
    }

    public String getMovieId() {
        return movieId;
    }

    public double getRating() {
        return rating;
    }

    @Override
    public String toString() {
        return movieId + "," + rating;
    }
}
