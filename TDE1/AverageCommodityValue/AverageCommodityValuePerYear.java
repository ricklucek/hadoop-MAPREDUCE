package TDE1.AverageCommodityValue;

import java.io.IOException;

import advanced.customwritable.AverageTemperature;
import advanced.customwritable.FireAvgTempWritable;
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

public class AverageCommodityValuePerYear {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "Average Commodity Value Per Year");

        // Registrar as classes
        j.setJarByClass(AverageCommodityValuePerYear.class);
        j.setMapperClass(AverageCommodityValuePerYear.MapCommodityPerYear.class);
        j.setReducerClass(AverageCommodityValuePerYear.ReduceForAverage.class);
        j.setCombinerClass(AverageCommodityValuePerYear.CombineForAverage.class);

        // Definir os tipos de saida

        // MAP
        j.setMapOutputKeyClass(CommodityYearWritable.class);
        j.setMapOutputValueClass(CostQuantityWritable.class);
        // REDUCE
        j.setOutputKeyClass(CommodityYearWritable.class);
        j.setOutputValueClass(DoubleWritable.class);

        // Definir os arquivos de entrada e de saída
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // Rodar
        j.waitForCompletion(false);
    }

    public static class MapCommodityPerYear extends Mapper<LongWritable, Text, CommodityYearWritable, CostQuantityWritable>{

        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException{

            // pula a primeira linha
            if (key.get() == 0) {
                return;
            }

            // Convertendo a linha de entrada em uma String
            String linha = value.toString();

            // Quebrando a linha em colunas
            String colunas[] = linha.split(";");

            // Obtendo os valores da Coluno Commodity_Type (3), Ano (2) e Value (6)
            String commodity = String.format(colunas[2]);
            double cost = Double.parseDouble(colunas[5]);
            String year = String.format(colunas[1]);

            int qtd = 1;

            // Mandando a (chave,valor) para o reduce
            con.write(
                    new CommodityYearWritable(commodity, year),
                    new CostQuantityWritable(cost, qtd)
            );

        }
    }

    public static class CombineForAverage extends Reducer<CommodityYearWritable, CostQuantityWritable, CommodityYearWritable, CostQuantityWritable>{
        public void reduce(CommodityYearWritable key, Iterable<CostQuantityWritable> values, Context con)
                throws IOException, InterruptedException {

            // Soma as ocorrencias e os custos

            double somaCusto = 0;
            int somaQtd = 0;

            for (CostQuantityWritable o : values){
                somaCusto += o.getCost();
                somaQtd += o.getQtd();
            }

            // Passando para o reduce valores pre-somados
            con.write(key, new CostQuantityWritable(somaCusto, somaQtd));
        }
    }

    public static class ReduceForAverage extends Reducer<CommodityYearWritable, CostQuantityWritable, CommodityYearWritable, DoubleWritable> {
        public void reduce(CommodityYearWritable key, Iterable<CostQuantityWritable> values, Context con)
                throws IOException, InterruptedException {

            // Logica do Reduce
            // recebe diferentes objetos compostos (custo e qtd)
            // somar custo e somar as qtds

            double somaCusto = 0;
            int somaQtd = 0;

            for(CostQuantityWritable o : values){
                somaCusto += o.getCost();
                somaQtd += o.getQtd();
            }

            // Custo médio
            double media = somaCusto / somaQtd;

            // Salvando o resultado
            con.write(key, new DoubleWritable(media));
        }
    }
}
