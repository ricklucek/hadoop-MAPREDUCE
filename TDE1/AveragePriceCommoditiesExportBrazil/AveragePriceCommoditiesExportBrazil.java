package TDE1.AveragePriceCommoditiesExportBrazil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class AveragePriceCommoditiesExportBrazil {
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
        j.setJarByClass(AveragePriceCommoditiesExportBrazil.class);
        j.setMapperClass(MapCommodityPerYear.class);
        j.setReducerClass(ReduceForAverage.class);
        j.setCombinerClass(CombineForAverage.class);

        // Definir os tipos de saida

        // MAP
        j.setMapOutputKeyClass(CommodityYearFlowWritable.class);
        j.setMapOutputValueClass(CostQuantityWritable.class);
        // REDUCE
        j.setOutputKeyClass(CommodityYearFlowWritable.class);
        j.setOutputValueClass(DoubleWritable.class);

        // Definir os arquivos de entrada e de saída
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // Rodar
        j.waitForCompletion(false);
    }

    public static class MapCommodityPerYear extends Mapper<LongWritable, Text, CommodityYearFlowWritable, CostQuantityWritable>{

        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException{

            // pula a primeira linha
            if (key.get() == 0) {
                return;
            }

            // Convertendo a linha de entrada em uma String
            String linha = value.toString();

            // Quebrando a linha em colunas
            String colunas[] = linha.split(";");

            // Obtendo os valores da Coluno Commodity_Type (3), Ano (2), Value (6) e Flow_type(5)
            String commodity = String.format(colunas[2]);
            double cost = Double.parseDouble(colunas[5]);
            String year = String.format(colunas[1]);
            String flow = String.format(colunas[4]);

            String pais = String.format(colunas[0]); // País deve ser Brasil

            int qtd = 1;

            // Verifica se o país é Brasil e manda a (chave,valor) para o reduce

            if (pais.equals("Brazil")) {

                con.write(
                        new CommodityYearFlowWritable(commodity, year, flow),
                        new CostQuantityWritable(cost, qtd)
                );
            }

        }
    }

    public static class CombineForAverage extends Reducer<CommodityYearFlowWritable, CostQuantityWritable, CommodityYearFlowWritable, CostQuantityWritable>{
        public void reduce(CommodityYearFlowWritable key, Iterable<CostQuantityWritable> values, Context con)
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

    public static class ReduceForAverage extends Reducer<CommodityYearFlowWritable, CostQuantityWritable, CommodityYearFlowWritable, DoubleWritable> {
        public void reduce(CommodityYearFlowWritable key, Iterable<CostQuantityWritable> values, Context con)
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
