package TDE1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

public class BrazilTransactions {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "Brazil Transactions counter");

        // Registro de classes
        j.setJarByClass(BrazilTransactions.class); // Classe que contém o método MAIN
        j.setMapperClass(MapCountryForCount.class); // Classe que contém o método MAP
        j.setReducerClass(ReduceCountryForCount.class); // Classe que contém o método REDUCE

        // Definir os tipos de saída
        j.setMapOutputKeyClass(Text.class); // tipo de chave de saída do MAP
        j.setMapOutputValueClass(IntWritable.class); // tiop de valor de saída do MAP

        j.setOutputKeyClass(Text.class); // tipo de chave de saída do REDUCE
        j.setOutputValueClass(IntWritable.class); // tipo de valor de saída do REDUCE

        // Definindo arquivos de entrada e saída
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // Executando a rotina
        j.waitForCompletion(false);

    }

    public static class MapCountryForCount extends Mapper<LongWritable, Text, Text, IntWritable>{

        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException{

            // Convertendo a linha de entrada em uma String
            String linha = value.toString();

            // Quebrando a linha em colunas
            String colunas[] = linha.split(";");

            // Obtendo os valores da 1° Coluno
            String pais = String.format(colunas[0]);

            // Verifica se o valor da coluna é igual a "Brazil"
            if (pais.equals("Brazil")) {
                String chave = pais;
                con.write(new Text(chave), new IntWritable(1));
            }
        }
    }

    public static class ReduceCountryForCount extends Reducer<Text, IntWritable, Text, IntWritable>{

        public void reduce(Text key, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException {

            // Somar as ocorrências
            int qtde = 0;
            for (IntWritable v: values){
                qtde += v.get();
            }

            // Salvando em disco
            con.write(key, new IntWritable(qtde));
        }
    }
}
