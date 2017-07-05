package com.company;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;

/**
 * メイン
 * Created by sekiguchikai on 2017/07/05.
 */
public class Main {
    public static void main(String[] args) {
        System.out.println("テスト");
        // Pipelineのoptionを定義
        // PipelineOptionsFactory.create : Creates and returns an object that implements {@link PipelineOptions}.
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();

        // Pipelineクラスのインスタンスを生成
        Pipeline pipeline = Pipeline.create();

        // PCollectionを作成
        PCollection<String> lines = pipeline.apply(
                "ReadMyFile", TextIO.read().from("protocol://path/to/some/inputData.txt"));

    }
}
