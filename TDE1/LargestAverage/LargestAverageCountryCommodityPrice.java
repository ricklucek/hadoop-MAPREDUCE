package TDE1.LargestAverage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;


public class LargestAverageCountryCommodityPrice {


    public static void main(String args[]) throws Exception {
        BasicConfigurator.configure();


        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();


        //Arquivo de entrada
        Path input = new Path(files[0]);


        //Arquivo de saida
        Path output = new Path(files[1]);

        //Criacao do job e seu nome
        Job j = new Job(c, "País com maior média de custo de exportação de commodity");


        //Registro de classes
        j.setJarByClass(LargestAverageCountryCommodityPrice.class);
        j.setMapperClass(MapForAverage.class);
        j.setReducerClass(Reduce.class);


        //Definição dos tipos de saida (map)
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(PriceQuantityWritable.class);

        //Reduce
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(MaxAverageWritable.class);

        JobConf jobConf = new JobConf();

        //Definiçao dos arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);


        //Lanca o job e aguarda sua execucao
        j.waitForCompletion(false);
    }




    public static class MapForAverage extends Mapper<LongWritable, Text, Text, PriceQuantityWritable> {


        //Funcao de map
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            // pula a primeira linha
            if (key.get() == 0) {
                return;
            }


            //Obtendo o conteúdo das linhas
            String linha = value.toString();


            //Quebrando a linha em campos
            String campos[] = linha.split(";");


            //Acessando a posição dos campos
            String pais = campos[0];

            double commodityusd = Double.parseDouble(campos[5]);

            String flow = campos[4];

            int qtd = 1;

            if (flow.equals("Export")){
                con.write(new Text(pais),
                        new PriceQuantityWritable(commodityusd, qtd));
            }
        }
    }

    public static class Reduce extends Reducer<Text, PriceQuantityWritable, Text, MaxAverageWritable>{

        public void reduce(Text key, Iterable<PriceQuantityWritable> values, Iterable<MaxAverageWritable> v, Context con)
                throws IOException, InterruptedException {

            // Soma as ocorrencias e os custos

            double somaCusto = 0;
            int somaQtd = 0;

            for (PriceQuantityWritable o : values){
                somaCusto += o.getCommodityusd();
                somaQtd += o.getQtd();
            }

            double avg = 0;
            if(somaCusto / somaQtd > avg){

                avg = somaCusto/ somaQtd;

            };

            float average = (float) avg;


            // Salvando em disco

            con.write(key, new MaxAverageWritable(average));
        }
    }
}

