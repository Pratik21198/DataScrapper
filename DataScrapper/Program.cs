using HtmlAgilityPack;
using System;
using System.IO;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using System.Threading;

namespace DataScrapper
{
  class Program
  {
    private static string sqlConnectionString = ConfigurationManager.AppSettings["SqlConnectionString"];
    static bool ResetPage = false;
    static void Main(string[] args)
    {
      try
      {
        DateTime startTime, endTime;
        startTime = DateTime.Now;

        DataTable dtCategories = GetCategories();
        int cat = 0;
        int pageNo = 1;
        while (cat < dtCategories.Rows.Count)
        {
          string Category = Convert.ToString(dtCategories.Rows[cat]["Category"]);
          string ListUrlBase = string.Format("{0}{1}",
                                              Convert.ToString(dtCategories.Rows[cat]["CategoryUrl"]),
                                              pageNo > 1 ? string.Format("?page={0}", pageNo) : "");
          Console.WriteLine(string.Format("Importing category: {0}", Category));
          Console.WriteLine("");
          ImportData(Category, ListUrlBase, pageNo);

          //move to next category
          pageNo = 1;
          cat++;
        }

        endTime = DateTime.Now;
        Console.WriteLine(string.Format("Started: {0}", startTime.ToString()));
        Console.WriteLine(string.Format("Finished: {0}", endTime.ToString()));
        Console.ReadKey();
      }
      catch (Exception ex)
      {
        Console.WriteLine(ex.Message);
        Console.ReadKey();
      }
    }

    static void ImportData(string Category, string ListUrlBase, int pageNo)
    {
      try
      {
        string ListUrl = ListUrlBase;
        bool hasProducts = true;
        HtmlWeb web = new HtmlWeb();

        while (hasProducts)
        {
          HtmlDocument productList = web.Load(ListUrl);
          hasProducts = pageNo == 1 || productList.DocumentNode.SelectSingleNode("//head/title").InnerText.Contains(Convert.ToString(pageNo));
          if (hasProducts)
          {
            IEnumerable<HtmlNode> products = productList.DocumentNode
                                                        .SelectNodes("//div[@class='plp-products']")[0]
                                                        .SelectNodes("//div[@class='card']");

            int currentLineCursor = Console.CursorTop - 1;
            Console.SetCursorPosition(0, Console.CursorTop);
            Console.Write(new string(' ', Console.WindowWidth));
            Console.SetCursorPosition(0, currentLineCursor);
            Console.WriteLine(string.Format("======================PAGE {0}======================", pageNo));

            List<Task> lstTask = new List<Task>();
            foreach (HtmlNode product in products)
            {
              Task task = Task.Run(() => ImportProduct(product, Category, pageNo));
              lstTask.Add(task);
            }

            Task.WaitAll(lstTask.ToArray());
          }
          if (ResetPage)
          {
            RemovePageData(Category, pageNo);
            ResetPage = false;
          }
          else
          {
            ListUrlBase = ListUrlBase.Replace(string.Format("?page={0}", pageNo), "");
            pageNo++;
            ListUrl = string.Format("{0}?page={1}", ListUrlBase, pageNo);
          }       
        }
      }
      catch (Exception ex)
      {
        throw ex;
      }
    }
    static void ImportProduct(HtmlNode product, string Category, int pageNo)
    {
      try
      {
        string BaseUrl = "https://meesho.com";
        string productUrl = string.Format("{0}{1}",
                                           BaseUrl,
                                           product.ChildNodes[0].ChildNodes[0].Attributes["href"].Value);

        HtmlWeb web = new HtmlWeb();
        HtmlDocument productPage = web.Load(productUrl);

        string imageUrl = productPage.DocumentNode
                                     .SelectSingleNode("//section[@class='ProductDisplay__ImageWrapper-sc-2h14i1-0 hIkCDs']")
                                     .ChildNodes[0].Attributes["src"].Value;

        string removeBaseImageURL = imageUrl.Replace("https://images.meesho.com/images/products/", "");

        string sku = string.Format("S-{0}", removeBaseImageURL.Substring(0, removeBaseImageURL.IndexOf('/')));

        using (WebClient imageClient = new WebClient())
        {
          Directory.CreateDirectory(string.Format(@"E:\_Storefuse\IMG\Products\{0}", Category.Replace(" > ", "\\")));
          imageClient.DownloadFile(imageUrl, string.Format(@"E:\_Storefuse\IMG\Products\{0}\{1}.jpg", Category.Replace(" > ", "\\"), sku));
        }

        string productName = productPage.DocumentNode
                                        .SelectSingleNode("//h1[@class='ShippingInfo__PdpTitle-sc-17v6v24-1 dHcaWM']")
                                        .InnerText.Replace("Trusted", "");

        string price = productPage.DocumentNode
                                  .SelectSingleNode("//div[@class='ShippingInfo__ActualCost-sc-17v6v24-3 izQIMN']")
                                  .InnerText.Substring(1);
        string mrp = "";

        HtmlNode maxRetailPrice = productPage.DocumentNode
                                             .SelectSingleNode("//div[@class='ShippingInfo__MrpCost-sc-17v6v24-8 hMzqvR']");
        if (maxRetailPrice != null && maxRetailPrice.ChildNodes.Count > 0)
        {
          mrp = maxRetailPrice.ChildNodes[0].InnerHtml.Trim().Substring(14);
        }

        string deliveryCharge = "";
        string dispatchETA = "";

        HtmlNode shippingInfo = productPage.DocumentNode
                                           .SelectSingleNode("//ul[@class='ShippingInfo__InfoProducts-sc-17v6v24-9 jtJOHC']");

        if (shippingInfo.ChildNodes.Count > 1)
        {
          deliveryCharge = shippingInfo.ChildNodes[0].ChildNodes[2].InnerText.Trim().Substring(1);
          dispatchETA = shippingInfo.ChildNodes[1].ChildNodes[1].InnerText;
        }
        else
        {
          dispatchETA = shippingInfo.ChildNodes[0].ChildNodes[1].InnerText;
        }

        HtmlNodeCollection sizeList = productPage.DocumentNode
                                       .SelectNodes("//div[@class='Prod_Avail_Info__ChipButton-sc-vkc520-3 jZZvYO']");
        string size = "";
        if (sizeList != null)
        { size = string.Join(" | ", sizeList.Select(i => i.InnerText)); }


        string description = string.Join(" | ", productPage.DocumentNode
                                                          .SelectSingleNode("//section[@class='ProductDescription__ProductDescriptionWrapper-sc-1rx3r0k-0 jmHdcm']")
                                                          .ChildNodes[1].ChildNodes.Select(i => string.Format("{0}{1}",
                                                                                                              i.ChildNodes[0].InnerText,
                                                                                                              i.ChildNodes[1].InnerText)));
        string soldBy = productPage.DocumentNode
                                   .SelectSingleNode("//div[@class='SupplierSection__SoldBy-sc-m2zjrn-3 djjTGt']")
                                   .InnerText;

        string sqlQuery = @"INSERT INTO [dbo].[MeeshoProducts]
                                 (
                                    [SKU]
                                   ,[Category]
                                   ,[Page]
                                   ,[ProductName]
                                   ,[DispatchETA]
                                   ,[Size]
                                   ,[Description]
                                   ,[SoldBy]
                                   ,[ImageUrl]
                                   ,[Price]
                                   ,[DeliveryCharge]
                                   ,[MRP]
                                 )
                                 VALUES(
                                    @SKU
                                   ,@Category
                                   ,@Page
                                   ,@ProductName
                                   ,@DispatchETA
                                   ,@Size
                                   ,@Description
                                   ,@SoldBy
                                   ,@ImageUrl
                                   ,@Price
                                   ,@DeliveryCharge
                                   ,@MRP
                                 )";

        using (SqlConnection cnn = new SqlConnection(sqlConnectionString))
        {
          using (SqlCommand cmd = new SqlCommand(sqlQuery, cnn))
          {
            cmd.Parameters.AddWithValue("@SKU", sku);
            cmd.Parameters.AddWithValue("@Category", Category);
            cmd.Parameters.AddWithValue("@Page", pageNo);
            cmd.Parameters.AddWithValue("@ProductName", productName);
            cmd.Parameters.AddWithValue("@DispatchETA", dispatchETA);
            cmd.Parameters.AddWithValue("@Size", size);
            cmd.Parameters.AddWithValue("@Description", description);
            cmd.Parameters.AddWithValue("@SoldBy", soldBy);
            cmd.Parameters.AddWithValue("@ImageUrl", imageUrl);
            cmd.Parameters.AddWithValue("@Price", price);
            cmd.Parameters.AddWithValue("@DeliveryCharge", string.IsNullOrWhiteSpace(deliveryCharge) ? "0" : deliveryCharge);
            cmd.Parameters.AddWithValue("@MRP", string.IsNullOrWhiteSpace(mrp) ? "0" : mrp);

            cnn.Open();
            cmd.ExecuteNonQuery();
            cnn.Close();
          }
        }
      }
      catch (Exception ex)
      {
        ResetPage = true;
      }
    }
    static DataTable GetCategories()
    {
      DataTable dtCategories = new DataTable();
      try
      {
        using (SqlConnection cnn = new SqlConnection(sqlConnectionString))
        {
          using (SqlDataAdapter da = new SqlDataAdapter("SELECT * FROM Categories WHERE IsActive = 1", cnn))
          {
            cnn.Open();
            da.Fill(dtCategories);
            cnn.Close();
          }
        }
        if (dtCategories == null || dtCategories.Rows.Count == 0)
        { throw new Exception("No eligible categories found..!"); }
      }
      catch (Exception ex)
      { throw ex; }
      return dtCategories;
    }
    static void RemovePageData(string category, int pageNo)
    {
      try
      {
        using (SqlConnection cnn = new SqlConnection(sqlConnectionString))
        {
          using (SqlCommand cmd = new SqlCommand(string.Format("DELETE MeeshoProducts WHERE Category = '{0}' AND Page = {1}", category, pageNo),
                                                cnn))
          {
            cnn.Open();
            cmd.ExecuteNonQuery();
            cnn.Close();
          }
        }
      }
      catch (Exception ex)
      {
        Console.WriteLine(ex.Message);
        Console.ReadKey();
      }
    }
  }
}
