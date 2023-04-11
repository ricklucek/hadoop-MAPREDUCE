package TDE1.FlowAndYear;

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

public class FlowAndYearTransactions {

    public static void main(String[] args) throws Exception{
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "Flow Type and Year Transactions counter");

        // Registrar as classes
        j.setJarByClass(FlowAndYearTransactions.class);
        j.setMapperClass(FlowAndYearTransactions.MapCountryForCount.class);
        j.setReducerClass(FlowAndYearTransactions.ReduceForCount.class);

        // Definir os tipos de saida

        // MAP
        j.setMapOutputKeyClass(FlowAndYearWritable.class);
        j.setMapOutputValueClass(IntWritable.class);
        // REDUCE
        j.setOutputKeyClass(FlowAndYearWritable.class);
        j.setOutputValueClass(IntWritable.class);

        // Definir os arquivos de entrada e de saída
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // Rodar
        j.waitForCompletion(false);
    }

    public static class MapCountryForCount extends Mapper<LongWritable, Text, FlowAndYearWritable, IntWritable>{

        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException{

            // Convertendo a linha de entrada em uma String
            String linha = value.toString();

            // Quebrando a linha em colunas
            String colunas[] = linha.split(";");

            // Obtendo os valores da Coluno 2 e Coluna 5
            String year = String.format(colunas[1]);
            String flowType = String.format(colunas[4]);

            IntWritable qtd = new IntWritable(1);
            // Mandando a chave-valor para o reduce
            con.write(new FlowAndYearWritable(flowType, year), qtd);
        }
    }

    public static class ReduceForCount extends Reducer<FlowAndYearWritable, IntWritable, FlowAndYearWritable, IntWritable>{
        public void reduce(FlowAndYearWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{

            // Criando variável de contagem
            int contagem = 0;

            // Varrendo as listas e incrementando
            for(IntWritable value : values){
                contagem += value.get();
            }

            // Salvando em disco
            context.write(key, new IntWritable(contagem));
        }
    }

}
