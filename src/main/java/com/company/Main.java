package com.company;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;


/**
 * メイン
 * Created by sekiguchikai on 2017/07/05.
 */
public class Main {
    // DoFnを実装したクラス
    // DoFnの横の<T,T>でinputとoutputの方の定義を行う
    // この場合、StringがinputでIntegerがoutput
    static class FilterEvenFn extends DoFn<String, String> {
        // 実際の処理ロジックにはこのアノテーションをつける
        @ProcessElement
        // ProcessContextは、inputを表すobject
        // 自分で定義しなくてもBeam SDKが勝手に取ってきてくれる
        public void processElement(ProcessContext c) {
            System.out.print(c.element());
            int num = Integer.parseInt(c.element());
            if (num % 2 == 0) {
                System.out.println("ifの結果" + num);
                c.output(String.valueOf(num));
            }
        }
    }

    // インプットデータのパス
    private static final String INPUT_FILE_PATH = "./dataflow_number_test.csv";
    // アウトデータのパス
    private static final String OUTPUT_FILE_PATH = "./sample.csv";

    public static void main(String[] args) {
        // まずPipelineに設定するOptionを作成する
        // 今回は、ローカルで起動するため、DirectRunnerを指定する
        // ローカルモードでは、DirectRunnerがすでにデフォルトになっているため、ランナーを設定する必要はない
        PipelineOptions options = PipelineOptionsFactory.create();

        // Optionを元にPipelineを生成する
        Pipeline pipeline = Pipeline.create(options);

        // inout dataを読み込んで、そこからPCollection(パイプライン内の一連のデータ)を作成する
        PCollection<String> stringData = pipeline.apply(TextIO.read().from(INPUT_FILE_PATH));

        // 処理
        PCollection<String> even = stringData.apply(ParDo.of(new FilterEvenFn()));
        // 書き込む
        even.apply(TextIO.write().to(OUTPUT_FILE_PATH));

        pipeline.run().waitUntilFinish();
    }
}