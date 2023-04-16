package TDE1.MaxMinMeanPricePerYearType;

import TDE1.MaxMinMeanPricePerYearType.MaxMinWritable;
import TDE1.MaxMinMeanPricePerYearType.PriceQuantityWritable;
import TDE1.MaxMinMeanPricePerYearType.TypeYearWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;


import java.io.IOException;


public class MaxMinYearTransaction {


    public static void main(String args[]) throws Exception {
        BasicConfigurator.configure();


        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();


        //Arquivo de entrada
        Path input = new Path(files[0]);


        //Arquivo de saida
        Path output = new Path(files[1]);

        //Criacao do job e seu nome
        Job j = new Job(c, "Máximo, mínimo e média por tipo e ano");


        //Registro de classes
        j.setJarByClass(MaxMinYearTransaction.class);
        j.setMapperClass(MapForAverage.class);
        j.setReducerClass(Reduce.class);


        //Definição dos tipos de saida (map)
        j.setMapOutputKeyClass(TypeYearWritable.class);
        j.setMapOutputValueClass(PriceQuantityWritable.class);


        //Reduce
        j.setOutputKeyClass(TypeYearWritable.class);
        j.setOutputValueClass(MaxMinWritable.class);


        //Definiçao dos arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);


        //Lanca o job e aguarda sua execucao
        j.waitForCompletion(false);
    }




    public static class MapForAverage extends Mapper<LongWritable, Text, TypeYearWritable, PriceQuantityWritable> {


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
            String ano = campos[1];
            double commodityusd = Double.parseDouble(campos[5]);
            String commodity = campos[2];

            int qtd = 1;


            con.write(new TypeYearWritable(commodity, ano),
                    new PriceQuantityWritable(commodityusd, qtd));
        }
    }

    public static class Reduce extends Reducer<TypeYearWritable, PriceQuantityWritable, TypeYearWritable, MaxMinWritable>{
        public void reduce(TypeYearWritable key, Iterable<PriceQuantityWritable> values, Context con)
                throws IOException, InterruptedException {

            // Soma as ocorrencias e encontra os valores máximos e mínimos

            double somaCusto = 0;
            int somaQtd = 0;

            double Max = 0;
            double Min = 99999;

            for (PriceQuantityWritable o : values){
                somaCusto += o.getCommodityusd();
                somaQtd += o.getQtd();

                if (o.getCommodityusd() > Max){
                    Max = o.getCommodityusd();
                }
                if (o.getCommodityusd() < Min){
                    Min = o.getCommodityusd();
                }
            }

            double avrg = somaCusto/somaQtd;
            float average = (float) avrg;



            // Salvando em disco
            con.write(key, new MaxMinWritable(Max, Min, average));
        }
    }
}

